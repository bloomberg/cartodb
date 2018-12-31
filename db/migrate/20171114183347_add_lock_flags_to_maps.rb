Sequel.migration do

  change do
    alter_table :maps do
      add_column :lock_pan, :boolean, :default => false
      add_column :lock_zoom, :boolean, :default => false
    end
  end

end