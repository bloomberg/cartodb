# encoding: utf-8

require 'helpers/metrics_helper'

require_relative '../../../spec_helper'
require_relative 'visualizations_controller_shared_examples'
require_relative '../../../../app/controllers/api/json/visualizations_controller'
require_relative '.././../../factories/organizations_contexts'
require 'factories/carto_visualizations'

describe Api::Json::VisualizationsController do
  include Carto::Factories::Visualizations

  it_behaves_like 'visualization controllers' do
  end

  include Rack::Test::Methods
  include Warden::Test::Helpers
  include CacheHelper
  include MetricsHelper

  # Mock for a Rails context
  class ContextMock
    def initialize(global_context)
      @global_context = global_context
    end

    def request
      nil
    end

    def polymorphic_path(*args)
      @global_context.polymorphic_path(*args)
    end
  end

  before(:all) do
    Carto::NamedMaps::Api.any_instance.stubs(:get => nil, :create => true, :update => true)
    @user = create_user(username: 'test')
    @mock_context = ContextMock.new(self)

    shared_empty_dataset_name = Cartodb.config[:shared_empty_dataset_name]
    commondata_username = Cartodb.config[:common_data]['username']
    @commondata_user = create_user(username: commondata_username)

    @shared_empty_dataset_vis = new_table({name: shared_empty_dataset_name, user_id: @commondata_user.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload.table_visualization
    @lib_vis = new_table({name: 'lib_dataset', user_id: @commondata_user.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload.table_visualization
  end

  after(:all) do
    bypass_named_maps
    @user.destroy
  end

  # let(:params) { { api_key: @user.api_key } }

  before(:each) do
    bypass_named_maps
    bypass_metrics

    host! "#{@user.username}.localhost.lan"
  end

  after(:each) do
    bypass_named_maps
    delete_user_data @user
  end

  describe '#create' do
    describe '#duplicate map' do
      before(:all) do
        @other_user = create_user
      end

      before(:each) do
        bypass_named_maps

        @map = Map.create(user_id: @user.id, table_id: create_table(user_id: @user.id).id)
        @visualization = FactoryGirl.create(:derived_visualization, map_id: @map.id, user_id: @user.id,
                                                                    privacy: Visualization::Member::PRIVACY_PRIVATE)
      end

      after(:each) do
        @map.destroy
      end

      after(:all) do
        @other_user.destroy
      end

      it 'duplicates a map' do
        new_name = @visualization.name + ' patatas'

        post_json api_v1_visualizations_create_url(api_key: @user.api_key),
                  source_visualization_id: @visualization.id,
                  name: new_name

        last_response.status.should be_success

        Carto::Visualization.exists?(user_id: @user.id, type: 'derived', name: new_name).should be_true
      end

      it 'registers table dependencies for duplicated maps' do
        map, table, table_visualization, visualization = create_full_visualization(Carto::User.find(@user.id))
        new_name = visualization.name + ' registered'

        post_json api_v1_visualizations_create_url(api_key: @user.api_key),
                  source_visualization_id: visualization.id,
                  name: new_name

        last_response.status.should be_success

        visualization = Carto::Visualization.where(user_id: @user.id, type: 'derived', name: new_name).first
        visualization.should be
        visualization.data_layers.first.user_tables.count.should eq 1

        destroy_full_visualization(map, table, table_visualization, visualization)
      end

      it "duplicates someone else's map if has at least read permission to it" do
        new_name = @visualization.name + ' patatas'

        Carto::Visualization.any_instance.stubs(:is_viewable_by_user?).returns(true)

        post_json api_v1_visualizations_create_url(user_domain: @other_user.username, api_key: @other_user.api_key),
                  source_visualization_id: @visualization.id,
                  name: new_name

        last_response.status.should be_success

        Carto::Visualization.exists?(user_id: @other_user.id, type: 'derived', name: new_name).should be_true
      end

      it "doesn't duplicate someone else's map without permission" do
        new_name = @visualization.name + ' patatatosky'

        post_json api_v1_visualizations_create_url(user_domain: @other_user.username, api_key: @other_user.api_key),
                  source_visualization_id: @visualization.id,
                  name: new_name

        last_response.status.should == 403

        Carto::Visualization.exists?(user_id: @other_user.id, type: 'derived', name: new_name).should be_false
      end
    end

    describe '#creates map from datasets' do
      include_context 'organization with users helper'
      include TableSharing

      it 'creates a visualization from a dataset given the viz id' do
        table1 = create_table(user_id: @org_user_1.id)
        payload = {
          source_visualization_id: table1.table_visualization.id
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch

          v.user.should eq @org_user_1
          v.map.user.should eq @org_user_1
        end
      end

      it 'creates a visualization from a dataset given the table id' do
        table1 = create_table(user_id: @org_user_1.id)
        payload = {
          tables: [table1.name]
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch

          v.user.should eq @org_user_1
          v.map.user.should eq @org_user_1
        end
      end

      it 'correctly creates a visualization from two dataset of different users' do
        table1 = create_table(user_id: @org_user_1.id)
        table2 = create_table(user_id: @org_user_2.id)
        share_table_with_user(table1, @org_user_2)
        payload = {
          type: 'derived',
          tables: ["#{@org_user_1.username}.#{table1.name}", table2.name]
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_2.username, api_key: @org_user_2.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch

          v.user.should eq @org_user_2
          v.map.user.should eq @org_user_2
        end
      end

      it 'copies the styles for editor users' do
        table1 = create_table(user_id: @org_user_1.id)
        payload = {
          tables: [table1.name]
        }
        User.any_instance.stubs(:builder_enabled?).returns(false)
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch
          original_layer = table1.map.data_layers.first
          layer = v.map.data_layers.first
          layer.options['tile_style'].should eq original_layer.options['tile_style']
        end
      end

      it 'resets the styles for builder users' do
        table1 = create_table(user_id: @org_user_1.id)
        Table.any_instance.stubs(:geometry_types).returns(['ST_Point'])
        payload = {
          tables: [table1.name]
        }
        User.any_instance.stubs(:builder_enabled?).returns(true)
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch

          original_layer = table1.map.data_layers.first
          layer = v.map.data_layers.first
          layer.options['tile_style'].should_not eq original_layer.options['tile_style']
        end
      end

      it 'doen\'t add style properites for editor users' do
        table1 = create_table(user_id: @org_user_1.id)
        payload = {
          tables: [table1.name]
        }
        User.any_instance.stubs(:builder_enabled?).returns(false)
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch

          layer = v.map.data_layers.first
          layer.options['style_properties'].should be_nil
        end
      end

      it 'adds style properites for builder users' do
        table1 = create_table(user_id: @org_user_1.id)
        Table.any_instance.stubs(:geometry_types).returns(['ST_Point'])
        payload = {
          tables: [table1.name]
        }
        User.any_instance.stubs(:builder_enabled?).returns(true)
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch

          layer = v.map.data_layers.first
          layer.options['style_properties'].should_not be_nil
        end
      end

      it 'rewrites queries for other user datasets' do
        table1 = create_table(user_id: @org_user_1.id)
        layer = table1.map.data_layers.first
        layer.options['query'] = "SELECT * FROM #{table1.name} LIMIT 1"
        layer.save
        share_table_with_user(table1, @org_user_2)
        payload = {
          type: 'derived',
          tables: ["#{@org_user_1.username}.#{table1.name}"]
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_2.username, api_key: @org_user_2.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch
          layer = v.map.data_layers.first
          layer.options['query'].should eq "SELECT * FROM #{@org_user_1.username}.#{table1.name} LIMIT 1"
        end
      end

      it 'does not rewrite queries for same user datasets' do
        table1 = create_table(user_id: @org_user_1.id)
        layer = table1.map.data_layers.first
        layer.options['query'] = "SELECT * FROM #{table1.name} LIMIT 1"
        layer.save
        share_table_with_user(table1, @org_user_1)
        payload = {
          type: 'derived',
          tables: ["#{@org_user_1.username}.#{table1.name}"]
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch
          new_layer = v.map.data_layers.first
          new_layer.options['query'].should eq layer.options['query']
        end
      end

      it 'sets table privacy if the user has private_maps' do
        table1 = create_table(user_id: @org_user_1.id)
        payload = {
          tables: [table1.name]
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_1.username, api_key: @org_user_1.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch
          v.privacy.should eq CartoDB::Visualization::Member::PRIVACY_PRIVATE
        end
      end

      it 'sets PUBLIC privacy if the user doesn\'t have private_maps' do
        @carto_org_user_2.update_column(:private_maps_enabled, false) # Direct to DB to skip validations
        table1 = create_table(user_id: @org_user_2.id)
        payload = {
          tables: [table1.name]
        }
        post_json(api_v1_visualizations_create_url(user_domain: @org_user_2.username, api_key: @org_user_2.api_key),
                  payload) do |response|
          response.status.should eq 200
          vid = response.body[:id]
          v = CartoDB::Visualization::Member.new(id: vid).fetch
          v.privacy.should eq CartoDB::Visualization::Member::PRIVACY_PUBLIC
        end
      end
    end
  end

  describe "#update" do
    before(:each) do
      login(@user)
    end

    it "Reverts privacy changes if named maps communitacion fails" do

      @user.private_tables_enabled = true
      @user.save

      table = new_table(user_id: @user.id, privacy: ::UserTable::PRIVACY_PUBLIC).save.reload

      Carto::NamedMaps::Api.any_instance
                           .stubs(:create)
                           .raises('manolos')

      put_json api_v1_visualizations_update_url(id: table.table_visualization.id),
      {
        visualization_id: table.table_visualization.id,
        privacy: Carto::Visualization::PRIVACY_PRIVATE
      }.to_json do |response|
        response.status.should_not be_success
        response.status.should eq 400
      end

      table.reload
      table.privacy.should eq ::UserTable::PRIVACY_PUBLIC

      @user.private_tables_enabled = false
      @user.save
    end

  end

  describe '#likes' do

    before(:each) do
      login(@user)
    end

    it "when a map is liked should send an email to the owner" do
      user_owner = create_user
      table = new_table({user_id: user_owner.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload
      vis, rejected_layers = CartoDB::Visualization::DerivedCreator.new(user_owner, [table]).create
      rejected_layers.empty?.should be true
      Resque.expects(:enqueue).with(::Resque::UserJobs::Mail::MapLiked, vis.id, @user.id, kind_of(String)).returns(true)
      post_json api_v1_visualizations_add_like_url({
          id: vis.id
        }) do |response|
        response.status.should be_success
      end
    end

    it "when a map is liked by the owner, the email should not be sent" do
      table = new_table({user_id: @user.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload
      vis, rejected_layers = CartoDB::Visualization::DerivedCreator.new(@user, [table]).create
      rejected_layers.empty?.should be true
      Resque.expects(:enqueue).with(::Resque::UserJobs::Mail::MapLiked, vis.id, @user.id, kind_of(String)).never
      post_json api_v1_visualizations_add_like_url({
          id: vis.id
        }) do |response|
        response.status.should be_success
      end
    end

    it "when a dataset is liked should send an email to the owner" do
      user_owner = create_user
      vis = new_table({user_id: user_owner.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload.table_visualization
      Resque.expects(:enqueue).with(::Resque::UserJobs::Mail::TableLiked, vis.id, @user.id, kind_of(String)).returns(true)
      post_json api_v1_visualizations_add_like_url({
          id: vis.id
        }) do |response|
        response.status.should be_success
      end
    end

    it "when a dataset is liked by the owner, the email should not be sent" do
      vis = new_table({user_id: @user.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload.table_visualization
      Resque.expects(:enqueue).with(::Resque::UserJobs::Mail::TableLiked, vis.id, @user.id, kind_of(String)).never
      post_json api_v1_visualizations_add_like_url({
          id: vis.id
        }) do |response|
        response.status.should be_success
      end
    end
  end

  describe '#subcategories endpoint' do
    before(:all) do
      clear_categories
      init_categories
    end

    before(:each) do
      login(@user)
    end

    after(:all) do
      clear_categories
    end

    it 'gets categories & sub categories' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_subcategories', {}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 0
      end

      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_subcategories', {category_id: @parentCat.parent_id}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 3
      end

      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_subcategories', {category_id: @parentCat.id}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 2
      end
    end

    it 'category - subcategory relationship established correctly' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_subcategories', {category_id: @parentCat.parent_id}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 3
        child_categories = categories.select { |category| category['parent_id'] == @parentCat.id }
        child_categories.count.should eq 2
        child_categories = categories.select { |category| category['id'] == @childCat1.id }
        child_categories.count.should eq 1
        child_categories = categories.select { |category| category['id'] == @childCat2.id }
        child_categories.count.should eq 1
      end
    end
  end

  describe '#list endpoint' do
    before(:all) do
      clear_categories
      init_categories
    end

    before(:each) do
      login(@user)

      @v1 = Visualization::Member.new(random_viz_attributes(user_id: @user.id, name: 'v1', locked:false, category: @childCat1[:id], tags: ['tagx', 'tagy'])).store
      @v2 = Visualization::Member.new(random_viz_attributes(user_id: @user.id, name: 'v2', locked:true, category: @childCat2[:id], tags: ['tagx'])).store
      @v3 = Visualization::Member.new(random_viz_attributes(user_id: @user.id, name: 'v3', locked:false, category: @childCat2[:id], tags: ['tagy'])).store
    end

    after(:all) do
      clear_categories
    end

    it 'gets visualizations' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz.count.should be >= 3
      end
    end

    it 'gets visualizations by parent category' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', parent_category: @parentCat[:id]}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz.count.should be >= 3
      end
    end

    it 'gets visualizations by sub-category' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', parent_category: @childCat1[:id]}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['id'] == @v1.id }
        viz.count.should eq 1
      end

      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', parent_category: @childCat2[:id]}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| [@v2.id, @v3.id].include? v['id'] }
        viz.count.should eq 2
      end
    end

    it 'gets visualizations by tag' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', tags: 'tagx'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| [@v1.id, @v2.id].include? v['id'] }
        viz.count.should eq 2
      end

      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', tags: 'tagy'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| [@v1.id, @v3.id].include? v['id'] }
        viz.count.should eq 2
      end

      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', tags: 'tagx,tagy'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz.count.should be >= 3
      end
    end

    it 'gets liked visualizations' do
      post_json api_v1_visualizations_add_like_url({
          id: @v1.id
        }) do |response|
        response.status.should be_success
      end

      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', only_liked: true}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['id'] == @v1.id }
        viz.count.should eq 1
      end
    end

    it 'gets locked visualizations' do
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table', locked: true}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['id'] == @v2.id }
        viz.count.should eq 1
      end
    end
  end

  describe 'commondata library datasets' do
    it 'shared_empty_dataset should be visible to commondata user' do
      login(@commondata_user)
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table,remote'}, @commondata_user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @shared_empty_dataset_vis.name }
        viz.count.should be 1
      end
    end

    it 'shared_empty_dataset should be hidden from regular user' do
      login(@user)
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table,remote'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @shared_empty_dataset_vis.name }
        viz.count.should be 0
      end
    end

    it 'library dataset should be visible to commondata user' do
      login(@commondata_user)
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table,remote'}, @commondata_user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @lib_vis.name }
        viz.count.should be 1
      end
    end

    it 'library dataset should be visible to regular user' do
      login(@user)
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'table,remote'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @lib_vis.name }
        viz.count.should be 1
      end
    end

    it 'library dataset should be visible to regular user with types=remote' do
      login(@user)
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'remote'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @lib_vis.name }
        viz.count.should be 1
      end
    end
  end

  describe 'load common data' do
    it 'common data datasets should return without login' do
      pending("HTTP requests from test environment is connecting to dev environment. Needs retest after HTTP request is fixed")
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_index', {types: 'table', privacy: ::UserTable::PRIVACY_PUBLIC}, @commondata_user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz.count.should be 2 #must include shared_empty_dataset
      end
    end

    it 'remote dataset syncing should work correctly' do
      pending("HTTP requests from test environment is connecting to dev environment. Needs retest after HTTP request is fixed")
      visualizations_api_url = CartoDB::Visualization::CommonDataService.build_url(self)
      @user.load_common_data(visualizations_api_url, false)
      login(@user)
      get_json CartoDB.url(@mock_context, 'api_v1_visualizations_list', {types: 'remote'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @lib_vis.name && v['needs_cd_import'] == false }
        viz.count.should be 1
      end
    end
  end

  def table_factory(attrs = {})
    new_table(attrs.merge(user_id: @user_1.id)).save.reload
  end

  def clear_categories
    Carto::Category.delete_all
  end

  def init_categories
    @parentCat = FactoryGirl.create(:category, id: 1, type: 1, name: 'Datasets', parent_id: 0)
    @childCat1 = FactoryGirl.create(:category, id: 2, type: 1, name: 'Energy', parent_id: 1)
    @childCat2 = FactoryGirl.create(:category, id: 3, type: 1, name: 'Infrastructure', parent_id: 1)
  end

  def random_viz_attributes(attributes={})
    random = unique_name('viz')
    {
      name:         attributes.fetch(:name, random),
      description:  attributes.fetch(:description, "description #{random}"),
      privacy:      attributes.fetch(:privacy, 'public'),
      tags:         attributes.fetch(:tags, ['tag 1']),
      type:         attributes.fetch(:type, CartoDB::Visualization::Member::TYPE_CANONICAL),
      user_id:      attributes.fetch(:user_id, UUIDTools::UUID.timestamp_create.to_s),
      locked:       attributes.fetch(:locked, false),
      category:     attributes.fetch(:category, nil)
    }
  end
end
