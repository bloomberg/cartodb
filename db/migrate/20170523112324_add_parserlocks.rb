Sequel.migration do
    change do
        create_table(:parserlocks) do
            String :parser, :primary_key=>true
            String :hostname
            Integer :pid
            DateTime :update_timestamp
        end
    end
    down do
        drop_table(:parserlocks)
    end
end
