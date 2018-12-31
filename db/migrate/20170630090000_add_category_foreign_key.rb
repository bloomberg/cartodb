Sequel.migration do
  up do
    execute "ALTER TABLE visualization_categories ADD CONSTRAINT unique_id UNIQUE (id);"
    execute "ALTER TABLE visualizations ADD CONSTRAINT visualizations_category_fkey
      FOREIGN KEY (category) REFERENCES visualization_categories(id);"
  end

  down do
    execute "ALTER TABLE visualizations DROP CONSTRAINT visualizations_category_fkey;"
    execute "ALTER TABLE visualizations DROP CONSTRAINT unique_id;"
  end
end