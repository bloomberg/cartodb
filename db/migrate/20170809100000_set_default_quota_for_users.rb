Sequel.migration do
  up do
    alter_table :users do
      set_column_default :quota_in_bytes, 262144000
    end
  end
  down do
    alter_table :users do
      set_column_default :quota_in_bytes, 104857600
    end
  end
end
