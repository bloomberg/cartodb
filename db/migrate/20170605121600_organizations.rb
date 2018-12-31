Sequel.migration do
  up do
    alter_table :organizations do
      set_column_default :geocoding_block_price, 0
      set_column_default :geocoding_quota, 2000000000
    end
  end
  down do
    alter_table :organizations do
      set_column_default :geocoding_block_price, nil
      set_column_default :geocoding_quota, 0
    end
  end
end
