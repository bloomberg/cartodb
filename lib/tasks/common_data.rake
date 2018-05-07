# coding: UTF-8
require 'json'

namespace :cartodb do
  namespace :common_data do
    desc 'Generates datasets assets and upload them to Amazon S3'
    task :generate_s3_assets, [:all_public] => :environment do |t, args|
      all_public = args[:all_public].blank? ? false : args[:all_public]
      common_data = CommonData.new
      common_data.upload_datasets_to_s3 all_public
    end

    desc 'Import all the common datasets from CartoDB into the local common data user account'
    task import_common_data: [:environment] do
      old_base_url = Cartodb.config[:common_data]["base_url"]
      # temporarily set base URL to remote so dataset URLs will be correct
      Cartodb.config[:common_data]["base_url"] = "https://common-data.carto.com"
      common_data = CommonData.new('https://common-data.carto.com/api/v1/viz?type=table&privacy=public')
      username = Cartodb.config[:common_data]["username"]
      user = User.find(:username=>username)
      raise "User #{username} not found" if not user
      datasets = common_data.datasets
      raise "No datasets found to import" if datasets.size == 0
      if user.table_quota and user.table_quota < datasets.size
        raise "Common data user #{username} has a table quota too low to import all common datasets"
      end
      failed_imports = 0
      datasets.each do |dataset|
        begin
            data_import = DataImport.create(:user_id => user.id, :data_source => dataset["url"])
            data_import.run_import!
            if not data_import.success
              puts "Dataset '#{dataset['name']}' failed to import. Error code: #{data_import.error_code}"
              failed_imports += 1
            end
        rescue => exception
            puts "Error importing dataset '#{dataset['name']}' : #{exception}"
            failed_imports += 1
        end
        ActiveRecord::Base.connection.close
      end
      # unset base URL when done
      if old_base_url
        Cartodb.config[:common_data]["base_url"] = old_base_url
      else
        Cartodb.config[:common_data].delete("base_url")
      end

      if failed_imports > 0
        puts "Failed to import #{failed_imports} of #{datasets.size} common datasets."
        raise "Failed to import any common datasets" if failed_imports == datasets.size
      end

      # (re-)load common datasets for all users
      CommonDataRedisCache.new.invalidate
      cds = CartoDB::Visualization::CommonDataService.new
      url = CartoDB.base_url(username) + "/api/v1/viz?type=table&privacy=public"
      User.each do |u|
        u.update(:last_common_data_update_date=>nil)
        u.save
        cds.load_common_data_for_user(u, url)
      end
    end

    desc 'Dump canonical layer styles for common data tables'
    task :dump_canonical_layer_styles, [:layer_name_file,:layer_output_file] => :environment do |task, args|
      raise "layer name file required" if !args[:layer_name_file]
      raise "layer output file required" if !args[:layer_output_file]

      layer_names = File.read(args[:layer_name_file]).split("\n")
      common_data_username = Cartodb.config[:common_data]['username']
      common_data_user = Carto::User.where(username: common_data_username).first

      def add_bind(binds, value)
        binds << value
        "?"
      end

      binds = []
      style_query = <<-EOQ
        with mapsdata_tables as (
          select
            v.name
          from visualizations v
          join users u
            on u.id = v.user_id
          where u.username = #{add_bind(binds, common_data_username)}
            and v.type = 'table'
            and v.privacy = 'public'
            and v.name in #{add_bind(binds, layer_names)}
        )
        ,canonical_layers as (
          select
            v.name as layer_name,
            l.infowindow::jsonb as infowindow,
            l.tooltip::jsonb as tooltip,
            l.options::jsonb as options
          from maps m
          join users u
            on m.user_id = u.id
          join visualizations v
            on v.map_id = m.id
          join mapsdata_tables t
            on t.name = v.name
          join layers_maps lm
            on lm.map_id = m.id
          join layers l
            on l.id = lm.layer_id
          where v.name <> 'shared_empty_dataset'
            and l.kind = 'carto'
            and u.username = #{add_bind(binds, common_data_username)}
        )
        select *
        from canonical_layers;
      EOQ

      layer_styles = Sequel::Model.db.fetch(
        style_query,
        *binds
      ).all

      def json(str)
        str ? JSON.parse(str) : {}
      end

      def map_infowindow(infowindow)
        infowindow = json(infowindow)
        infowindow["fields"] && infowindow["fields"].each do |field|
          field.delete("position")
        end
        infowindow
      end

      def map_tooltip(tooltip)
        tooltip = json(tooltip)
        tooltip["fields"] && tooltip["fields"].each do |field|
          field.delete("position")
        end
        tooltip
      end

      def lookup_table(layer_name, user)
        user_table = Carto::Helpers::TableLocator.new.get_by_id_or_name(layer_name, user)
        table = user_table.service
        schema = table.schema(reload: true)
        sys_columns = [:the_geom, :the_geom_webmercator, :cartodb_id]
        column_names = schema.map { |col_info| col_info[0] } - sys_columns

        {
          uniqueKeys: ['cartodb_id'],
          columns: column_names.map { |col_name|
            {
              columnName: col_name,
              columnLabel: col_name,
              sorttype: "",
              filtertype: ""
            }
          }
        }
      end

      layer_digest = layer_styles.map do |layer|
        options = json(layer[:options])
        query = options["query"]

        {
          layer_name: layer[:layer_name],
          infowindow: map_infowindow(layer[:infowindow]),
          tooltip:    map_tooltip(layer[:tooltip]),
          legend:     options["legend"] || {},
          css:        options["tile_style"] || "",
          query:      query == "" ?  "SELECT * FROM #{layer[:layer_name]}" : query,
          table:      lookup_table(layer[:layer_name], common_data_user)
        }
      end

      puts "Writing canonical layer styles to #{args[:layer_output_file]}..."
      File.open(args[:layer_output_file], 'w') do |file|
        file.write(JSON.pretty_generate(layer_digest) + "\n")
      end
      puts "Done."
    end

  end
end
