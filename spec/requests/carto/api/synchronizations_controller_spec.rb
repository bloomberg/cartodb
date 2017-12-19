# encoding: utf-8

require 'sequel'
require 'rack/test'
require_relative '../../../spec_helper'
require_relative '../../api/json/synchronizations_controller_shared_examples'
require_relative '../../../../app/controllers/carto/api/synchronizations_controller'

describe Carto::Api::SynchronizationsController do
  include Rack::Test::Methods
  include Warden::Test::Helpers
  include CacheHelper

  it_behaves_like 'synchronization controllers' do
  end

  describe 'main behaviour' do
    # INFO: this tests come from spec/requests/api/json/synchronizations_controller_spec.rb

    before(:all) do
      @old_resque_inline_status = Resque.inline
      Resque.inline = false
      @user = create_user(
        sync_tables_enabled: true
      )
      @api_key = @user.api_key
    end

    before(:each) do
      @db = Rails::Sequel.connection
      Sequel.extension(:pagination)

      CartoDB::Synchronization.repository = DataRepository::Backend::Sequel.new(@db, :synchronizations)

      bypass_named_maps
      delete_user_data @user
      @headers = {
        'CONTENT_TYPE' => 'application/json'
      }
      host! CartoDB.base_url(@user.username).sub!(/^https?\:\/\//, '')
    end

    after(:all) do
      Resque.inline = @old_resque_inline_status
      bypass_named_maps
      @user.destroy
    end

    let(:params) { { :api_key => @api_key } }

    describe 'GET /api/v1/synchronizations/:id' do
      it 'returns a synchronization record' do
        payload = {
          table_name: 'table_1',
          interval:   3600,
          url:        'http://www.foo.com'
        }

        post api_v1_synchronizations_index_url(params), payload.to_json, @headers
        id = JSON.parse(last_response.body).fetch('id')

        get api_v1_synchronizations_show_url(params.merge(id: id)), nil, @headers
        last_response.status.should == 200

        response = JSON.parse(last_response.body)
        response.fetch('id').should == id
        response.fetch('url').should == payload.fetch(:url)
      end

      it 'returns 404 for unknown synchronizations' do
        get api_v1_synchronizations_show_url(params.merge(id: "56b40691-541b-4ef3-96da-f2be29563566")), nil, @headers
        last_response.status.should == 404
      end
    end

    describe 'GET /api/v1/synchronizations/:id/sync_now' do
      it 'returns sync status' do
        payload = {
          table_name: 'table_1',
          interval:   3600,
          url:        'http://www.foo.com'
        }

        post api_v1_synchronizations_index_url(params), payload.to_json, @headers
        id = JSON.parse(last_response.body).fetch('id')

        get api_v1_synchronizations_syncing_url(params.merge(id: id)), nil, @headers
        last_response.status.should == 200

        response = JSON.parse(last_response.body)
        response.fetch('state').should == 'created'
      end
    end

    describe 'GET /api/v1/synchronizations/' do
      it 'returns sync list' do
        get api_v1_synchronizations_index_url(params), nil, @headers
        last_response.status.should == 200
      end
    end
  end

end
