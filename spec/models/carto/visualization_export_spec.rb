require 'spec_helper'
require 'support/factories/users'
require 'helpers/named_maps_helper'

describe Carto::DataExporter do
  include CartoDB::Factories
  include NamedMapsHelper

  describe '#export_tables' do
    include_context 'user helper'

    # INFO: this needs SQL API configured for testing
    it 'exports a table in requested format' do
      table_name = 'table1'
      table = create_table(name: table_name, user_id: @user.id)
      user_table = Carto::UserTable.find_by_table_id(table.get_table_id)
      table.insert_row!(id: 666, description: 'desc', name: 'a row')

      format = 'shp'

      tmp_dir = "/tmp/export_test_#{String.random(15)}"
      FileUtils.mkdir_p tmp_dir

      begin
        Carto::Http::Request.any_instance.stubs(:run)
        # Bloomberg - Sample 2.0 changed the prototype to add a is_remote for FDW export.
        #   Set to false since carto unit tests have no concept of FDW
        file = File.new(Carto::DataExporter.new.export_table(user_table, tmp_dir, format, false))
        file.path.should match(/.#{format}$/)
        file.close
      ensure
        File.delete file if file
        FileUtils.rm_rf tmp_dir
        table.destroy
      end
    end
  end
end

describe Carto::VisualizationExport do
  include Carto::ExporterConfig
  include_context 'users helper'
  include Carto::Factories::Visualizations

  class FakeCartoHttpClientFileToucher
    attr_reader :touched_files

    def initialize
      @touched_files = []
    end

    # Mock needs not url nor Typhoeus options
    def get_file(_, exported_file, _)
      @touched_files << exported_file
      touch(exported_file)
    end

    private

    def random_filename(dir, extension: 'shp')
      "#{dir}/test_#{String.random(12)}.#{extension}".downcase
    end

    def touch_random_filename(dir, extension: 'shp')
      touch(random_filename(dir, extension: extension))
    end

    def touch(path)
      FileUtils.touch(path).first
    end
  end

  class FakeCartoHttpClientGpkgFileToucher < FakeCartoHttpClientFileToucher
      def get_file(_, exported_file, _)
        @touched_files << exported_file
        touch(exported_file)
      end

      def touch(path)
        # Create a gpkg file with the metadata table and data table.
        # The data table doesn't need the correct columns for purpose of test
        slash_pos = path.rindex('/')
        ext_pos = path.rindex('.')
        if slash_pos && ext_pos
          table_name = path[slash_pos +1..ext_pos-1]
        else
          table_name = path
        end
        db = SQLite3::Database.new(path)
        db.execute <<-SQL
          CREATE TABLE gpkg_metadata (id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC NOT NULL UNIQUE,md_scope TEXT NOT NULL DEFAULT 'dataset',md_standard_uri TEXT NOT NULL,mime_type TEXT NOT NULL DEFAULT 'text/xml',metadata TEXT NOT NULL);
        SQL
        db.execute <<-SQL
          CREATE TABLE #{table_name} (id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC NOT NULL UNIQUE);
        SQL
      end
  end

  let(:base_dir) { ensure_clean_folder('/tmp/exporter_test') }

  describe '#export' do
    it 'exports a .carto file including the carto.json and the files' do
      map = FactoryGirl.create(:carto_map, user: @carto_user1)

      table1 = FactoryGirl.create(:private_user_table, user: @carto_user1)
      table2 = FactoryGirl.create(:private_user_table, user: @carto_user1)

      layer1 = FactoryGirl.create(:carto_layer, options: { table_name: table1.name }, maps: [map])
      FactoryGirl.create(:carto_layer, options: { table_name: table2.name }, maps: [map])

      map, table, table_visualization, visualization = create_full_visualization(@carto_user1,
                                                                                 map: map,
                                                                                 table: table1,
                                                                                 data_layer: layer1)

      fake_carto_http_client_toucher = FakeCartoHttpClientFileToucher.new

      exported_file = Carto::VisualizationExport.new.export(
        visualization,
        @carto_user1,
        base_dir: base_dir,
        data_exporter: Carto::DataExporter.new(fake_carto_http_client_toucher))

      touched_files = fake_carto_http_client_toucher.touched_files
      CartoDB::Importer2::Unp.new.open(exported_file) do |files|
        files.length.should eq (map.layers.count + 1)
        names = files.map(&:path)
        names.count { |f| f =~ /\.carto\.json$/ }.should eq 1
        names.should include(touched_files[0].split('/').last)
        names.should include(touched_files[1].split('/').last)
      end

      ([exported_file] + touched_files).map { |f| File.delete(f) if File.exists?(f) }

      destroy_full_visualization(map, table, table_visualization, visualization)
    end

    it 'exports a .carto file including the carto.json and the carto.gpkg file for commond shared dataset' do
      map = FactoryGirl.create(:carto_map, user: @carto_user1)
      # Create the common_data_user from the config
      # TODO - Will this affect other unit tests?
      common_data_user = FactoryGirl.create(:carto_user, { username: Cartodb.config[:common_data]['username'] } )

      table1 = FactoryGirl.create(:private_user_table, user: @carto_user1)
      table2 = FactoryGirl.create(:private_user_table, user: @carto_user1)
      # Create a common dataset
      table3 = FactoryGirl.create(:private_user_table, user: @carto_user1)
      table3_visualization = create_table_visualization(common_data_user, table3)

      # Needs to create the synchronization record since that is how we identify common datasets
      data_import = FactoryGirl.create(:remote_synchronization, user_id: common_data_user.id, name: table3.name, visualization_id: table3_visualization.id)

      layer1 = FactoryGirl.create(:carto_layer, options: { table_name: table1.name }, maps: [map])
      FactoryGirl.create(:carto_layer, options: { table_name: table2.name }, maps: [map])
      FactoryGirl.create(:carto_layer, options: { table_name: table3.name }, maps: [map])

      map, table, table_visualization, visualization = create_full_visualization(@carto_user1,
                                                                                 map: map,
                                                                                 table: table1,
                                                                                 data_layer: layer1)

      # Need to create valid gpkg files
      fake_carto_http_client_toucher = FakeCartoHttpClientGpkgFileToucher.new

      exported_file = Carto::VisualizationExport.new.export(
        visualization,
        @carto_user1,
        base_dir: base_dir,
        data_exporter: Carto::DataExporter.new(fake_carto_http_client_toucher))

      touched_files = fake_carto_http_client_toucher.touched_files
      # Need to modify the filename of table3 to expected filename
      for i in (0...touched_files.length)
        if touched_files[i] =~ /#{table3.name}/
          touched_files[i].sub! '.gpkg', '.carto.gpkg'
        end
      end

      CartoDB::Importer2::Unp.new.open(exported_file) do |files|
        files.length.should eq (map.layers.count + 1)
        names = files.map(&:path)
        names.count { |f| f =~ /\.carto\.json$/ }.should eq 1
        names.count { |f| f =~ /\.carto\.gpkg$/ }.should eq 1
        names.should include(touched_files[0].split('/').last)
        names.should include(touched_files[1].split('/').last)
        names.should include(touched_files[2].split('/').last)

        # Validate the metadata of the carto.gpkg file
        for f in files.map(&:fullpath)
          if f =~/\.carto\.gpkg$/
            Carto::GpkgCartoMetadataUtil.open(geopkg_file: f) do |md|
              expected_md = {
                                   vendor: 'carto',
                                   data: {
                                       source: {
                                             type: 'fdw',
                                               configuration: {
                                                   parent_table: "#{table3.name}"
                                                   }
                                               }
                                       }
                                   }.with_indifferent_access
              md.metadata.should eq expected_md
            end
          end
        end
      end

      ([exported_file] + touched_files).map { |f| File.delete(f) if File.exists?(f) }

      destroy_full_visualization(map, table, table_visualization, visualization)
    end

    it 'excludes data not accessible by the user' do
      map = FactoryGirl.create(:carto_map, user: @carto_user1)

      table1 = FactoryGirl.create(:private_user_table, user: @carto_user1)

      layer1 = FactoryGirl.create(:carto_layer, options: { table_name: table1.name }, maps: [map])

      map, table, table_visualization, visualization = create_full_visualization(@carto_user1,
                                                                                 map: map,
                                                                                 table: table1,
                                                                                 data_layer: layer1)

      fake_carto_http_client_toucher = FakeCartoHttpClientFileToucher.new

      exported_file = Carto::VisualizationExport.new.export(
        visualization,
        @carto_user2,
        base_dir: base_dir,
        data_exporter: Carto::DataExporter.new(fake_carto_http_client_toucher))

      touched_files = fake_carto_http_client_toucher.touched_files
      CartoDB::Importer2::Unp.new.open(exported_file) do |files|
        files.length.should eq 1 # visualization export
        names = files.map(&:path)
        names.count { |f| f =~ /\.carto\.json$/ }.should eq 1
      end

      ([exported_file] + touched_files).map { |f| File.delete(f) if File.exists?(f) }

      destroy_full_visualization(map, table, table_visualization, visualization)
    end

    it 'excludes layers and user_tables with user_tables_ids parameter' do
      map = FactoryGirl.create(:carto_map, user: @carto_user1)

      table1 = FactoryGirl.create(:private_user_table, user: @carto_user1)
      table2 = FactoryGirl.create(:private_user_table, user: @carto_user1)

      layer1 = FactoryGirl.create(:carto_layer, options: { table_name: table1.name }, maps: [map])
      FactoryGirl.create(:carto_layer, options: { table_name: table2.name }, maps: [map])

      map, table, table_visualization, visualization = create_full_visualization(@carto_user1,
                                                                                 map: map,
                                                                                 table: table1,
                                                                                 data_layer: layer1)

      fake_carto_http_client_toucher = FakeCartoHttpClientFileToucher.new

      exported_file = Carto::VisualizationExport.new.export(
        visualization,
        @carto_user1,
        user_tables_ids: [table1.id],
        base_dir: base_dir,
        data_exporter: Carto::DataExporter.new(fake_carto_http_client_toucher))

      touched_files = fake_carto_http_client_toucher.touched_files
      CartoDB::Importer2::Unp.new.open(exported_file) do |files|
        files.length.should eq (1 + 1) # selected user_table + metadata
        names = files.map(&:path)
        names.count { |f| f =~ /\.carto\.json$/ }.should eq 1
        names.should include(touched_files[0].split('/').last)
        touched_files.length.should eq 1
      end

      ([exported_file] + touched_files).map { |f| File.delete(f) if File.exists?(f) }

      destroy_full_visualization(map, table, table_visualization, visualization)
    end
  end

  describe '#run_export!' do
    # run_export! is called from the job, but the core of the logic is in `#export`, this makes sure its usage
    it 'calls #export' do
      visualization = FactoryGirl.create(:carto_visualization, user: @carto_user1)
      ve = FactoryGirl.create(:visualization_export, user: @carto_user1, visualization: visualization)

      fake_path = "/tmp/fakepath"
      touch(fake_path)
      ve.expects(:export).returns(fake_path)
      file_upload_helper_mock = mock
      file_upload_helper_mock.expects(:upload_file_to_storage)

      ve.run_export!(file_upload_helper: file_upload_helper_mock)

      visualization.destroy
      File.delete(fake_path) if File.exists?(fake_path)
    end

    describe 'with S3' do
      before(:each) do
        @visualization = FactoryGirl.create(:carto_visualization, user: @carto_user1)
      end

      after(:each) do
        @visualization.destroy
      end

      let(:test_dir) { '/tmp/exports_test/' }
      let(:fake_path) { "#{test_dir}/fake_export.carto" }

      it 'runs export creating a log trace' do
        ve = Carto::VisualizationExport.new(visualization: @visualization, user: @carto_user1)

        FileUtils.mkdir_p test_dir
        FileUtils.touch(fake_path).first
        ve.expects(:export).returns(fake_path)

        file_upload_helper_mock = mock
        file_upload_helper_mock.expects(:upload_file_to_storage).returns(file_url: fake_path)

        ve.run_export!(file_upload_helper: file_upload_helper_mock)
        ve.reload

        ve.log.should_not be_nil
        ve.destroy
      end
    end
  end
end
