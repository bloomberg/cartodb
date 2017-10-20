Sequel.migration do
    up do
        add_column :parserlocks, :health_check_url, String, :null=>true
    end

    down do
        drop_column :parserlocks, :health_check_url
    end
end
