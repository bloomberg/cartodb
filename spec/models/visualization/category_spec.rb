# encoding: utf-8
require_relative '../../spec_helper'
require_relative '../../../services/data-repository/backend/sequel'
require_relative '../../../services/data-repository/repository'
require_relative '../../../app/models/carto/category'
require_relative '../../../app/models/visualization/collection'
require 'helpers/unique_names_helper'

include UniqueNamesHelper
include CartoDB

describe Carto::Category do
  before(:all) do
    clear_categories
    @user = FactoryGirl.create(:carto_user)
    @parentCat = FactoryGirl.create(:category, id: 1, type: 1, name: 'Datasets', parent_id: 0)
    @childCat1 = FactoryGirl.create(:category, id: 2, type: 1, name: 'Energy', parent_id: 1)
    @childCat2 = FactoryGirl.create(:category, id: 3, type: 1, name: 'Infrastructure', parent_id: 1)
  end

  before(:each) do
    Varnish.any_instance.stubs(:send_command).returns(true)
    @db = Rails::Sequel.connection
    Visualization.repository  = DataRepository::Backend::Sequel.new(@db, :visualizations)

    CartoDB::NamedMapsWrapper::NamedMaps.any_instance.stubs(:get => nil, :create => true, :update => true)
  end

  after(:all) do
    clear_categories
  end

  describe 'initialization' do
    it 'visualization categories should be properly initialized' do
      categories = Carto::Category.all

      categories.count.should eq 3
      Carto::Category.where(parent_id: @parentCat.id).count.should eq 2
    end

    it 'visualization should have default category as nil' do
      v1 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v1', locked:true)).store

      vqb1 = Carto::VisualizationQueryBuilder.new.with_id(v1.id).build
      vqb1.first[:category].should eq nil

      v1.delete
    end

    it 'should search by category correctly' do
      v1 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v1', locked:true, category: @childCat1[:id])).store
      v2 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v2', locked:true, category: @childCat2[:id])).store

      vqb1 = Carto::VisualizationQueryBuilder.new.with_parent_category(@childCat1[:id]).build.map(&:id)
      vqb1.should include v1.id

      vqb2 = Carto::VisualizationQueryBuilder.new.with_parent_category(@childCat2[:id]).build.map(&:id)
      vqb2.should include v2.id

      v1.delete
      v2.delete
    end
  end

  describe 'searching' do
    it 'should search by parent category correctly' do
      v1 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v1', locked:true, category: @childCat1[:id])).store
      v2 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v2', locked:true, category: @childCat2[:id])).store

      vqb1 = Carto::VisualizationQueryBuilder.new.with_parent_category(@parentCat).build.map(&:id)

      vqb1.should include v1.id
      vqb1.should include v2.id

      v1.delete
      v2.delete
    end

    it 'should search by category correctly' do
      v1 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v1', locked:true, category: @childCat1[:id])).store
      v2 = Visualization::Member.new(random_attributes(user_id: @user.id, name: 'v2', locked:true, category: @childCat2[:id])).store

      vqb1 = Carto::VisualizationQueryBuilder.new.with_parent_category(@childCat1[:id]).build.map(&:id)
      vqb1.should include v1.id

      vqb2 = Carto::VisualizationQueryBuilder.new.with_parent_category(@childCat2[:id]).build.map(&:id)
      vqb2.should include v2.id

      v1.delete
      v2.delete
    end
  end

  def clear_categories
    Carto::Category.delete_all
  end

  def random_attributes(attributes={})
    random = unique_name('viz')
    result = {
      name:         attributes.fetch(:name, random),
      description:  attributes.fetch(:description, "description #{random}"),
      privacy:      attributes.fetch(:privacy, 'public'),
      tags:         attributes.fetch(:tags, ['tag 1']),
      type:         attributes.fetch(:type, CartoDB::Visualization::Member::TYPE_CANONICAL),
      user_id:      attributes.fetch(:user_id, UUIDTools::UUID.timestamp_create.to_s),
      locked:       attributes.fetch(:locked, false)
    }
    category = attributes.fetch(:category, nil)
    if category
      result[:category] = category
    end
    result
  end
end
