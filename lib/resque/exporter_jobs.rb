# encoding: utf-8
require_relative './base_job'

module Resque
  class ExporterJobs < BaseJob
    @queue = :exports

    def self.perform(options = {})
      run_action(options, @queue, lambda do |options|
        download_path = options['download_path']
        name_suffix = options['name_suffix']
        Carto::VisualizationExport.find(options.symbolize_keys[:job_id]).run_export!(download_path: download_path, name_suffix: name_suffix)
      end)
    end
  end
end
