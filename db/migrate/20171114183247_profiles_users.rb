Sequel.migration do

  up do
    create_table(:profiles_users) do
      primary_key [:user_id, :profile_id], :name => :profiles_users_user_id_profile_id_pkey
      foreign_key :profile_id, :profiles, :type=>"uuid", :null=>false, :key=>[:id]
      foreign_key :user_id, :users, :type=>"uuid", :null=>false, :key=>[:id]
    end
  end

  down do
    drop_table :profiles_users
  end

end
