require_relative '../spec_helper'

describe 'pgconnectionsTest' do

    before(:all) do
        @users = 9
        ::User.all.each(&:destroy)
        @user = Array.new
        for current_iteration_number in 0..@users do
            user = create_user(username: "test#{current_iteration_number}", email: "client#{current_iteration_number}@example.com", password: "clientex")
            @user.push(user)
        end
        stub_named_maps_calls
    end

    after(:all) do
        stub_named_maps_calls
        for current_iteration_number in 0..@users do
            @user[current_iteration_number].destroy
        end
    end

    it "import data test" do
        for current_iteration_number in 0..@users do
            user = @user[current_iteration_number]
            data_import = DataImport.create(
              user_id: user.id,
              data_source: '/../db/fake_data/clubbing.csv',
              updated_at: Time.now
            )
            data_import.run_import!
            data_import.state.should be == 'complete'
        end
    end
end

