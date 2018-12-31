Sequel.migration do
  change do
    alter_table :users do
      set_column_default :geocoding_block_price, 0
    end
  end
  down do
    alter_table :users do
      set_column_default :geocoding_block_price, nil
    end
  end
end
