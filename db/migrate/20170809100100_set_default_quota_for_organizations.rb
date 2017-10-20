Sequel.migration do
  up do
    alter_table :organizations do
      set_column_default :default_quota_in_bytes, 262144000
    end
  end
  down do
    alter_table :organizations do
      set_column_default :default_quota_in_bytes, 104857600
    end
  end
end
