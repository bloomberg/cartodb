Sequel.migration do 
  change do

    create_table :user_infos do
      Integer :uuid, null: false
      Text :username
      Text :email
      Text :firstname
      Text :lastname
      Integer :firm_id
    end

  end
end
