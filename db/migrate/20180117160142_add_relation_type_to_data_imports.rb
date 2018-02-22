require 'carto/db/migration_helper'

include Carto::Db::MigrationHelper

migration(
  # up
  Proc.new do
    alter_table :data_imports do
      add_column :relation_type, :text, null: false, default: 'table'
    end
  end,
  # down
  Proc.new do
    alter_table :data_imports do
      drop_column :relation_type
    end
  end
)

