namespace :cartodb do
  namespace :templates do

    desc 'Compute total size of user templates stored in redis in bytes'
    task :compute_user_template_size => :environment do

      # Query keys which store all metadata for map templates
      tmpl_keys = $tables_metadata.keys('map_tpl|*')
      batch_size = 1000
      total_tmpl_size = 0

      tmpl_key_batches = {}
      tmpl_key_batches.default_proc = lambda { |k, v| [] }
      tmpl_keys.each_with_index do |k, i|
        tmpl_key_batches[i / batch_size] = tmpl_key_batches[i / batch_size] << k
      end

      tmpl_key_batches.each_value do |ks|
        tmpl_debugs = []

        $tables_metadata.multi do
          ks.each do |k|
             tmpl_debugs << $tables_metadata.debug('object', k)
          end
        end

        total_tmpl_size += tmpl_debugs.reduce(0) do |sum, debug|
          # Debug object output consists of a list of key value pairs
          # of the form <key>:<value>.  The 'serializedlength' entry
          # gives the size in bytes of the stored object.
          sum + debug.value[/serializedlength:[0-9]+/][/[0-9]+/].to_i
        end
      end

      puts "Total users: #{tmpl_keys.size}"
      puts "Total template size in bytes: #{total_tmpl_size}"

    end

  end
end