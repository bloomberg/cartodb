Sequel.migration do
  up do
    alter_table :visualizations do
      set_column_default :category, nil
    end
    execute "UPDATE visualizations SET category=NULL WHERE category=-1;"
  end
  down do
    alter_table :visualizations do
      set_column_default :category, -1
    end
    execute "UPDATE visualizations SET category=-1 WHERE category IS NULL;"
  end
end
