Sequel.migration do

  up do
    create_table :profiles do
      column :id, "uuid", :default=>Sequel::LiteralString.new("uuid_generate_v4()"),
             :null=>false, :primary_key=>true
      column :name, "text", :null=>false, :unique=>true
      column :attrs, "jsonb", :null=>false
    end
  end

  down do
    drop_table :profiles
  end

end