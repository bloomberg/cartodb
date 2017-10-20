# encoding: utf-8

require_relative '../../../spec_helper'
require_relative 'visualizations_controller_shared_examples'
require_relative '../../../../app/controllers/api/json/visualizations_controller'

describe Api::Json::VisualizationsController do
  it_behaves_like 'visualization controllers' do
  end

  include Rack::Test::Methods
  include Warden::Test::Helpers
  include CacheHelper

  before(:all) do
    CartoDB::NamedMapsWrapper::NamedMaps.any_instance.stubs(:get => nil, :create => true, :update => true)
    @user = create_user(username: 'test')

    shared_empty_dataset_name = Cartodb.config[:shared_empty_dataset_name]
    commondata_username = Cartodb.config[:common_data]['username']
    @commondata_user = create_user(username: commondata_username)

    @shared_empty_dataset_vis = new_table({name: shared_empty_dataset_name, user_id: @commondata_user.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload.table_visualization
    @lib_vis = new_table({name: 'lib_dataset', user_id: @commondata_user.id, privacy: ::UserTable::PRIVACY_PUBLIC}).save.reload.table_visualization
  end

  after(:all) do
    stub_named_maps_calls
    @user.destroy
  end

  # let(:params) { { api_key: @user.api_key } }

  before(:each) do
    stub_named_maps_calls
    host! "#{@user.username}.localhost.lan"
  end

  after(:each) do
    stub_named_maps_calls
    delete_user_data @user
  end

  describe "#update" do
    before(:each) do
      login(@user)
    end

    it "Reverts privacy changes if named maps communitacion fails" do

      @user.private_tables_enabled = true
      @user.save

      table = new_table(user_id: @user.id, privacy: ::UserTable::PRIVACY_PUBLIC).save.reload

      CartoDB::NamedMapsWrapper::NamedMaps.any_instance
                                          .stubs(:create)
                                          .raises(CartoDB::NamedMapsWrapper::HTTPResponseError)

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
      get_json CartoDB.url(self, 'api_v1_visualizations_subcategories', {}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 0
      end

      get_json CartoDB.url(self, 'api_v1_visualizations_subcategories', {category_id: @parentCat.parent_id}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 3
      end

      get_json CartoDB.url(self, 'api_v1_visualizations_subcategories', {category_id: @parentCat.id}, @user) do |response|
        response.status.should be_success
        categories = response.body
        categories.count.should eq 2
      end
    end

    it 'category - subcategory relationship established correctly' do
      get_json CartoDB.url(self, 'api_v1_visualizations_subcategories', {category_id: @parentCat.parent_id}, @user) do |response|
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
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz.count.should be >= 3
      end
    end

    it 'gets visualizations by parent category' do
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', parent_category: @parentCat[:id]}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz.count.should be >= 3
      end
    end

    it 'gets visualizations by sub-category' do
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', parent_category: @childCat1[:id]}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['id'] == @v1.id }
        viz.count.should eq 1
      end

      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', parent_category: @childCat2[:id]}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| [@v2.id, @v3.id].include? v['id'] }
        viz.count.should eq 2
      end
    end

    it 'gets visualizations by tag' do
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', tags: 'tagx'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| [@v1.id, @v2.id].include? v['id'] }
        viz.count.should eq 2
      end

      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', tags: 'tagy'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| [@v1.id, @v3.id].include? v['id'] }
        viz.count.should eq 2
      end

      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', tags: 'tagx,tagy'}, @user) do |response|
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

      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', only_liked: true}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['id'] == @v1.id }
        viz.count.should eq 1
      end
    end

    it 'gets locked visualizations' do
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table', locked: true}, @user) do |response|
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
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table,remote'}, @commondata_user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @shared_empty_dataset_vis.name }
        viz.count.should be 1
      end
    end

    it 'shared_empty_dataset should be hidden from regular user' do
      login(@user)
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table,remote'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @shared_empty_dataset_vis.name }
        viz.count.should be 0
      end
    end

    it 'library dataset should be visible to commondata user' do
      login(@commondata_user)
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table,remote'}, @commondata_user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @lib_vis.name }
        viz.count.should be 1
      end
    end

    it 'library dataset should be visible to regular user' do
      login(@user)
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'table,remote'}, @user) do |response|
        response.status.should be_success
        viz = response.body[:visualizations]
        viz = viz.select { |v| v['name'] == @lib_vis.name }
        viz.count.should be 1
      end
    end

    it 'library dataset should be visible to regular user with types=remote' do
      login(@user)
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'remote'}, @user) do |response|
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
      get_json CartoDB.url(self, 'api_v1_visualizations_index', {types: 'table', privacy: ::UserTable::PRIVACY_PUBLIC}, @commondata_user) do |response|
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
      get_json CartoDB.url(self, 'api_v1_visualizations_list', {types: 'remote'}, @user) do |response|
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
