require 'date'

namespace :cartodb do

  namespace :remotes do

    task :clear, [:username] => [:environment] do |t, args|
      username = args[:username]
      raise 'username required' unless username.present?

      u = ::User.where(username: username).first
      require_relative '../../app/services/visualization/common_data_service'
      deleted = CartoDB::Visualization::CommonDataService.new.delete_common_data_for_user(u)
      puts "Deleted #{deleted} remote visualizations"
    end

    task :clear_org, [:org_name] => [:environment] do |t, args|
      org_name = args[:org_name]
      raise 'organization name required' unless org_name.present?

      require_relative '../../app/services/visualization/common_data_service'
      common_data_service = CartoDB::Visualization::CommonDataService.new
      o = Organization.where(name: org_name).first
      o.users.each { |u|
        common_data_service.delete_common_data_for_user(u)
      }
    end

    desc 'Load common data account remotes. Pass username as first argument. Example: `rake cartodb:remotes:reload[development]`'
    task :reload, [:username] => [:environment] do |t, args|
      username = args[:username]
      raise 'username required' unless username.present?

      u = ::User.where(username: username).first
      require_relative '../../app/services/visualization/common_data_service'
      vis_api_url = get_visualizations_api_url
      CartoDB::Visualization::CommonDataService.new.load_common_data_for_user(u, vis_api_url)
    end

    desc 'Load common data account remotes for a whole organization. Pass organization name as first argument. Example: `rake cartodb:remotes:reload[my_team]`'
    task :reload_org, [:org_name] => [:environment] do |t, args|
      org_name = args[:org_name]
      raise 'organization name required' unless org_name.present?

      require_relative '../../app/services/visualization/common_data_service'
      common_data_service = CartoDB::Visualization::CommonDataService.new
      vis_api_url = get_visualizations_api_url
      o = Organization.where(name: org_name).first
      o.users.each {|u|
        common_data_service.load_common_data_for_user(u, vis_api_url)
      }
    end

    desc 'Load common data account remotes for multiple users, in alphabetical order. If you pass a username, it will do it beginning in the next username'
    task :load_all, [:from_username] => [:environment] do |t, args|
      require_relative '../../app/services/visualization/common_data_service'
      common_data_service = CartoDB::Visualization::CommonDataService.new
      vis_api_url = get_visualizations_api_url
      puts DateTime.now
      # TODO: batch
      users = ::User.order_by(:username)
      users = users.where("username > '#{args[:from_username]}'") unless args[:from_username].nil?
      users.all.each do |user|
        added, updated, not_modified, removed, failed = common_data_service.load_common_data_for_user(user, vis_api_url)
        printf("%20s: +%03d; *%03d; =%03d; -%03d; e%03d\n", user.username, added, updated, not_modified, removed, failed)
      end
      puts DateTime.now
    end

    desc "Invalidate user's date flag and make them refresh data library"
    task :invalidate_common_data => [:environment] do
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      invalidate_sql = %Q[
          UPDATE users
          SET last_common_data_update_date = null
          WHERE last_common_data_update_date >= now() - '#{::User::COMMON_DATA_ACTIVE_DAYS} day'::interval;
        ]
      updated_rows = Rails::Sequel.connection.fetch(invalidate_sql).update
      CommonDataRedisCache.new.invalidate
      puts "#{updated_rows} users invalidated"

      # Now we try to add the new common-data request to the cache using the common_data user
      common_data_user = ::User.where(username: Cartodb.config[:common_data]["username"]).first
      if !common_data_user.nil?
        vis_api_url = get_visualizations_api_url
        CartoDB::Visualization::CommonDataService.new.load_common_data_for_user(common_data_user, vis_api_url)
      end
    end

    desc "Initialize Visualization Categories"
    task :init_dataset_categories => [:environment] do
      forced_reset = ENV['forced_reset'] == "true"

      if forced_reset
        puts "Initializing with forced_reset"
        Rails::Sequel.connection.run("ALTER TABLE visualizations DROP CONSTRAINT visualizations_category_fkey;")
        Rails::Sequel.connection.run("DELETE FROM visualization_categories;")
        Rails::Sequel.connection.run("ALTER SEQUENCE visualization_categories_id_seq RESTART;")
        Rails::Sequel.connection.run("UPDATE visualization_categories SET id=DEFAULT;")
      end

      Rails::Sequel.connection.run("INSERT INTO visualization_categories (id, type, name, parent_id, list_order) VALUES
          (-1, 1, 'UNASSIGNED', 0, 0),
          (0, 1, 'ROOT', 0, 0),

          (1, 1, 'Datasets', 0, 0),
          (2, 2, 'Maps', 0, 1),

          (3, 1, 'Energy', 1, 0),
          (4, 1, 'Vessels', 1, 0),
          (5, 1, 'Environmental', 1, 0),
          (6, 1, 'Banking', 1, 0),
          (7, 1, 'Retail', 1, 0),
          (8, 1, 'Points of Interest', 1, 0),
          (9, 1, 'Administrative', 1, 0),
          (10, 1, 'Political', 1, 0),
          (11, 1, 'Infrastructure', 1, 0),
          (12, 1, 'Communications', 1, 0),

          (13, 1, 'Exploration', 3, 0),
          (14, 1, 'Renewable Energy', 3, 0),
          (15, 1, 'Coal', 3, 0),
          (16, 1, 'Natural Gas', 3, 0),
          (17, 1, 'Oil', 3, 0),
          (18, 1, 'Regions', 3, 0),
          (19, 1, 'Agriculture', 3, 0),
          (20, 1, 'Power', 3, 0),
          (21, 1, 'Metals', 3, 0),

          (22, 1, 'Natural Disasters', 5, 0),
          (23, 1, 'Climate', 5, 0),
          (24, 1, 'Administration', 5, 0),
          (25, 1, 'Weather', 5, 0),

          (26, 1, 'Global', 9, 0),
          (27, 1, 'Oceana', 9, 0),
          (28, 1, 'Asia', 9, 0),
          (29, 1, 'South America', 9, 0),
          (30, 1, 'Europe', 9, 0),
          (31, 1, 'North America', 9, 0);
      ")

      if forced_reset
        Rails::Sequel.connection.run("ALTER TABLE visualizations ADD CONSTRAINT visualizations_category_fkey
            FOREIGN KEY (category) REFERENCES visualization_categories(id);")
      end
    end

    desc "Sync category set in Data Library for all datasets to all users"
    task :sync_dataset_categories => [:environment] do
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      common_data_user = Cartodb.config[:common_data]["username"]

      lib_datasets = Hash[
        Rails::Sequel.connection.fetch(%Q[
          SELECT name, category FROM visualizations WHERE
            user_id=(SELECT id FROM users WHERE username='#{common_data_user}')
            AND type='remote';
        ]).all.map { |row| [row.fetch(:name), row.fetch(:category)] }
      ]

      lib_datasets.each { |dataset_name, dataset_category|
        sql_query = %Q[
          UPDATE visualizations SET category=#{dataset_category} WHERE name='#{dataset_name}';
          ]
        updated_rows = Rails::Sequel.connection.fetch(sql_query).update
        CommonDataRedisCache.new.invalidate
        puts "#{updated_rows} datasets named #{dataset_name} set to category #{dataset_category}"
      }
    end

    desc "Sync category set in Data Library to all users"
    task :sync_dataset_category, [:dataset_name] => [:environment] do |t, args|
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      common_data_user = Cartodb.config[:common_data]["username"]

      lib_datasets = Hash[
        Rails::Sequel.connection.fetch(%Q[
          SELECT name, category FROM visualizations WHERE
            user_id=(SELECT id FROM users WHERE username='#{common_data_user}')
            AND type='remote' AND name='#{args[:dataset_name]}';
        ]).all.map { |row| [row.fetch(:name), row.fetch(:category)] }
      ]

      lib_datasets.each { |dataset_name, dataset_category|
        sql_query = %Q[
          UPDATE visualizations SET category=#{dataset_category} WHERE name='#{dataset_name}';
          ]
        updated_rows = Rails::Sequel.connection.fetch(sql_query).update
        CommonDataRedisCache.new.invalidate
        puts "#{updated_rows} datasets named #{dataset_name} set to category #{dataset_category}"
      }
    end

    desc "Set dataset category in Data Library and propagate to all users"
    task :set_dataset_category, [:dataset_name, :dataset_category] => [:environment] do |t, args|
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      sql_query = %Q[
        UPDATE visualizations SET category=#{args[:dataset_category]} WHERE name='#{args[:dataset_name]}' AND (type='table' OR type='remote');
      ]
      updated_rows = Rails::Sequel.connection.fetch(sql_query).update
      CommonDataRedisCache.new.invalidate
      puts "#{updated_rows} datasets named #{args[:dataset_name]} set to category #{args[:dataset_category]}"
    end

    desc "Set dataset category by name in Data Library and propagate to all users"
    task :set_dataset_category_by_name, [:dataset_name, :dataset_category_name] => [:environment] do |t, args|
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      category_records = Rails::Sequel.connection.fetch(%Q[
          SELECT id FROM visualization_categories WHERE type=1 AND name='#{args[:dataset_category_name]}';
        ]).all

      if category_records.length == 1
        sql_query = %Q[
          UPDATE visualizations SET category=#{category_records[0][:id]} WHERE name='#{args[:dataset_name]}' AND (type='table' OR type='remote');
        ]
        updated_rows = Rails::Sequel.connection.fetch(sql_query).update
        CommonDataRedisCache.new.invalidate
        puts "#{updated_rows} datasets named #{args[:dataset_name]} set to category #{args[:dataset_category_name]}"
      else
        puts "Error!: #{category_records.length} categories found with name #{args[:dataset_category_name]}"
      end
    end

    desc "Sync dataset aliases for user"
    task :sync_dataset_aliases_for_user, [:dataset_name, :username] => [:environment] do |t, args|
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      common_data_user = Cartodb.config[:common_data]["username"]

      lib_datasets = Hash[
        Rails::Sequel.connection.fetch(%Q[
          SELECT name_alias, column_aliases FROM user_tables WHERE
            user_id=(SELECT id FROM users WHERE username='#{common_data_user}')
            AND name='#{args[:dataset_name]}';
        ]).all.map { |row| [row.fetch(:name_alias), row.fetch(:column_aliases)] }
      ]

      lib_datasets.each { |name_alias, column_aliases|
        sql_query = %Q[
          UPDATE user_tables SET name_alias='#{name_alias}', column_aliases='#{column_aliases}'::json WHERE
            user_id=(SELECT id FROM users WHERE username='#{args[:username]}') AND name='#{args[:dataset_name]}';
          ]
        updated_rows = Rails::Sequel.connection.fetch(sql_query).update
        puts "#{updated_rows} datasets named #{args[:dataset_name]} updated for user #{args[:username]}"
      }
    end

    desc "Sync dataset aliases for all users"
    task :sync_dataset_aliases, [:dataset_name] => [:environment] do |t, args|
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      common_data_user = Cartodb.config[:common_data]["username"]

      lib_datasets = Hash[
        Rails::Sequel.connection.fetch(%Q[
          SELECT name_alias, column_aliases FROM user_tables WHERE
            user_id=(SELECT id FROM users WHERE username='#{common_data_user}')
            AND name='#{args[:dataset_name]}';
        ]).all.map { |row| [row.fetch(:name_alias), row.fetch(:column_aliases)] }
      ]

      lib_datasets.each { |name_alias, column_aliases|
        sql_query = %Q[
          UPDATE user_tables SET name_alias='#{name_alias}', column_aliases='#{column_aliases}'::json WHERE
            name='#{args[:dataset_name]}' AND user_id <> (SELECT id FROM users WHERE username='#{common_data_user}');
          ]
        updated_rows = Rails::Sequel.connection.fetch(sql_query).update
        puts "Aliases for dataset named #{args[:dataset_name]} updated for #{updated_rows} users"
      }
    end

    desc "Sync dataset description and source set in Data Library to all users"
    task :sync_dataset_desc_and_source, [:dataset_name] => [:environment] do |t, args|
      require_relative '../../app/helpers/common_data_redis_cache'
      require_relative '../../app/services/visualization/common_data_service'

      name = args[:dataset_name]
      common_data_user = Cartodb.config[:common_data]["username"]

      lib_datasets = Hash[
        Rails::Sequel.connection.fetch(%Q[
          SELECT name, description, source FROM visualizations WHERE
            user_id=(SELECT id FROM users WHERE username='#{common_data_user}')
            AND type='remote' AND name='#{name}';
        ]).all.map { |row| [row.fetch(:name), {:description => row.fetch(:description), :source => row.fetch(:source)}] }
      ]

      if lib_datasets.count == 1
        dataset = lib_datasets[name]
        description = dataset[:description]
        source = dataset[:source]
        sql_query = %Q[
          UPDATE visualizations SET description='#{description}', source='#{source}'
          WHERE id IN (
            SELECT v.id FROM visualizations AS v
              LEFT JOIN synchronizations AS s ON s.visualization_id=v.id
              LEFT JOIN external_data_imports AS edi ON edi.synchronization_id=s.id
            WHERE v.name='#{name}' AND v.type='table' AND edi.id IS NOT NULL AND
            v.user_id<>(SELECT id FROM users WHERE username='#{common_data_user}')
          );
        ]
        updated_rows = Rails::Sequel.connection.fetch(sql_query).update
        CommonDataRedisCache.new.invalidate
        puts "#{updated_rows} datasets named '#{name}' assigned description '#{description}' and source '#{source}'"
      else
        puts "Error! Dataset not found..."
      end
    end

    desc "Sync dataset description, source, category, exportability and aliases set in Data Library to all users"
    task :sync_dataset_props, [:dataset_name] => [:environment] do |t, args|
      if ENV['verbose'] != "true"
        ActiveRecord::Base.logger = nil
      end

      name = args[:dataset_name]
      common_data_username = Cartodb.config[:common_data]["username"]
      lib_datasets = {}

      common_data_user = Carto::User.find_by_username(common_data_username)
      Carto::Visualization.where(user_id: common_data_user.id, type: 'table', privacy: 'public', name: name).each do |vis|
        category = vis.vis_category
        user_table = vis.user_table
        lib_datasets[vis.name] = {
          description: vis.description,
          source: vis.source,
          category: category.id,
          category_name: category.name,
          exportable: vis.exportable,
          export_geom: vis.export_geom,
          name_alias: user_table.name_alias,
          column_aliases: user_table.column_aliases
        }
      end

      if lib_datasets.count == 1
        dataset = lib_datasets[name]
        description = dataset[:description]
        source = dataset[:source]
        category = dataset[:category]
        category_name = dataset[:category_name]
        exportable = dataset[:exportable]
        export_geom = dataset[:export_geom]
        name_alias = dataset[:name_alias]
        column_aliases = dataset[:column_aliases] || {}

        # only update datasets with same name and imported from library, skip library user
        vis_ids = Carto::Visualization.includes(synchronization: :external_data_imports)
          .where(type: 'table', name: name)
          .where('external_data_imports.id IS NOT NULL')
          .where('visualizations.user_id <> ?', common_data_user.id)
          .select('visualizations.id')
          .all

        if vis_ids.empty?
          puts "Warning! No datasets with name '#{name}' found in user accounts"
        else
          updated_rows = Carto::Visualization.where(id: vis_ids)
                          .update_all(description: description, source: source, category: category, exportable: exportable, export_geom: export_geom)
          puts "#{updated_rows} '#{name}' datasets set description: '#{description}', source: '#{source}', category: '#{category_name}' (#{category}), exportable: #{exportable}, export_geom: #{export_geom}"
        end

        # only update dataset tables with same name and imported from library, skip library user
        ut_ids = Carto::UserTable.includes(visualization: { synchronization: :external_data_imports })
          .where(name: name)
          .where('external_data_imports.id IS NOT NULL')
          .where('user_tables.user_id <> ?', common_data_user.id)
          .select('user_tables.id')
          .all

        if ut_ids.empty?
          puts "Warning! No user tables with name '#{name}' found in user accounts"
        else
          updated_rows = Carto::UserTable.where(id: ut_ids)
                          .update_all(name_alias: name_alias, column_aliases: column_aliases.to_json)
          puts "#{updated_rows} '#{name}' datasets set name_alias: '#{name_alias}', column_aliases: '#{column_aliases}'"
        end
      else
        puts "Error! No dataset with name '#{name}' found in common-data account"
      end
    end

    def get_visualizations_api_url
      common_data_config = Cartodb.config[:common_data]
      username = common_data_config["username"]
      base_url = common_data_config["base_url"].nil? ? CartoDB.base_url(username) : common_data_config["base_url"]
      base_url + "/api/v1/viz?type=table&privacy=public"
    end

  end

end
