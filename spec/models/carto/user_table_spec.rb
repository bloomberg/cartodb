# coding: UTF-8
require_relative '../../spec_helper_min'
require 'models/user_table_shared_examples'

describe Carto::UserTable do
  include UniqueNamesHelper

  before(:all) do
    bypass_named_maps

    @user = FactoryGirl.create(:carto_user)
    @carto_user = @user

    @user_table = Carto::UserTable.new
    @user_table.user = @user
    @user_table.name = unique_name('user_table')
    @user_table.save

    # The dependent visualization models are in the UserTable class for the AR model
    @dependent_test_object = @user_table
  end

  after(:all) do
    @user_table.destroy
    @user.destroy
  end

  it_behaves_like 'user table models' do
    def build_user_table(attrs = {})
      ut = Carto::UserTable.new
      ut.assign_attributes(attrs, without_protection: true)
      ut
    end
  end

  describe '#default_privacy' do
    it 'sets privacy to nil by default' do
      expect(Carto::UserTable.new.privacy).to be_nil
    end

    it 'lets caller specify privacy' do
      [UserTable::PRIVACY_PRIVATE, UserTable::PRIVACY_LINK, UserTable::PRIVACY_PUBLIC].each do |privacy|
        expect(Carto::UserTable.new(privacy: privacy).privacy).to eq privacy
      end
    end
  end

  describe '#readable_by?' do
    include_context 'organization with users helper'
    include TableSharing

    it 'returns true for shared tables' do
      @table = create_table(privacy: UserTable::PRIVACY_PRIVATE, name: "a_table_name", user_id: @org_user_1.id)
      user_table = Carto::UserTable.find(@table.id)
      share_table_with_user(@table, @org_user_2)

      user_table.readable_by?(@carto_org_user_2).should be_true
    end
  end

  describe('#affected_visualizations') do
    before(:each) do
      # We recreate an inconsistent state where a layer has no visualization
      @user_table.stubs(:layers).returns([Carto::Layer.new])
    end

    describe('#fully_dependent_visualizations') do
      it 'resists layers without visualizations' do
        expect { @user_table.fully_dependent_visualizations }.to_not raise_error
      end
    end

    describe('#accessible_dependent_derived_maps') do
      it 'resists layers without visualizations' do
        expect { @user_table.accessible_dependent_derived_maps }.to_not raise_error
      end
    end

    describe('#partially_dependent_visualizations') do
      it 'resists layers without visualizations' do
        expect { @user_table.partially_dependent_visualizations }.to_not raise_error
      end
    end
  end

  describe('#name_alias') do
    let(:name_alias) { 'Manolo Escobar' }

    after(:all) do
      @user_table.update_attributes!(name_alias: nil)
    end

    it 'sets and gets' do
      @user_table.update_attributes!(name_alias: name_alias)
      @user_table.reload.name_alias.should eq(name_alias)
    end
  end

  describe('#column_aliases') do
    let(:column_aliases) do
      {
        one_column: 'with an alias',
        another_column: 'with another alias'
      }.with_indifferent_access
    end

    before(:each) do
      @user_table.update_attributes!(column_aliases: {})
    end

    after(:all) do
      @user_table.update_attributes!(column_aliases: {})
    end

    it 'sets and gets' do
      @user_table.update_attributes!(column_aliases: column_aliases)
      @user_table.reload.column_aliases.should eq column_aliases
    end

    it 'ignores format issues' do
      @user_table.update_attributes!(column_aliases: 'not a hash')
      @user_table.reload.column_aliases.should(eq({}))
    end

    it 'ignores nil issues' do
      @user_table.update_attributes!(column_aliases: nil)
      @user_table.reload.column_aliases.should(eq({}))
    end
  end
end
