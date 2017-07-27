require_relative 'visualization_presenter'
require_dependency 'carto/api/vizjson_presenter'
require_relative '../../../models/visualization/stats'
require_relative 'paged_searcher'
require_relative '../controller_helper'
require_dependency 'carto/uuidhelper'
require_dependency 'static_maps_url_helper'
require_relative 'vizjson3_presenter'

module Carto
  module Api
    class VisualizationsController < ::Api::ApplicationController
      include VisualizationSearcher
      include PagedSearcher
      include Carto::UUIDHelper
      include Carto::ControllerHelper
      include VisualizationsControllerHelper

      ssl_required :index, :show
      ssl_allowed  :vizjson2, :vizjson3, :likes_count, :likes_list, :is_liked, :list_watching, :static_map, :search, :list, :subcategories

      # TODO: compare with older, there seems to be more optional authentication endpoints
      skip_before_filter :api_authorization_required, only: [:index, :vizjson2, :vizjson3, :is_liked, :static_map]
      before_filter :optional_api_authorization, only: [:index, :vizjson2, :vizjson3, :is_liked, :static_map]

      before_filter :id_and_schema_from_params
      before_filter :load_by_name_or_id, only: [:vizjson2, :vizjson3]
      before_filter :load_visualization, only: [:likes_count, :likes_list, :is_liked, :show, :stats, :list_watching,
                                                :static_map]
      before_filter :load_common_data, only: [:index, :list]

      rescue_from Carto::LoadError, with: :rescue_from_carto_error
      rescue_from Carto::UUIDParameterFormatError, with: :rescue_from_carto_error

      LIST_VALID_VIS_TYPES = ["derived", "table", "remote"]
      LIST_VALID_ORDER_COLS = ["updated_at", "name", "description", "source", "likes"]

      def show
        render_jsonp(to_json(@visualization))
      rescue KeyError
        head(404)
      end

      def load_common_data
        return true unless current_user.present? && current_user.should_load_common_data?
        begin
          visualizations_api_url = CartoDB::Visualization::CommonDataService.build_url(self)
          current_user.load_common_data(visualizations_api_url, true)
        rescue Exception => e
          # We don't block the load of the dashboard because we aren't able to load common data
          CartoDB.notify_exception(e, {user:current_user})
          return true
        end
      end

      def index
        page, per_page, order = page_per_page_order_params
        types, total_types = get_types_parameters
        vqb = query_builder_with_filter_from_hash(params)
        hideSharedEmptyDataset = false
        emptyDatasetName = ''
        if current_user && !current_user.has_feature_flag?('bbg_disabled_shared_empty_dataset') then
          emptyDatasetName = Cartodb.config[:shared_empty_dataset_name]
          if current_user[:username] != Cartodb.config[:common_data]['username'] && params[:name] != emptyDatasetName then
            hideSharedEmptyDataset = true
          end
        end
        parent_category = params.fetch('parent_category', -1)
        asc_order = params.fetch('asc_order', 'false')
        name = params[:name]

        presenter_cache = Carto::Api::PresenterCache.new

        if hideSharedEmptyDataset then
          # TODO: undesirable table hardcoding, needed for disambiguation. Look for
          # a better approach and/or move it to the query builder
          excludedNames = [emptyDatasetName]
          query = vqb.with_order("visualizations.#{order}", asc_order == 'true' ? :asc : :desc).with_excluded_names(excludedNames)
          if parent_category != -1
            query = query.with_parent_category(parent_category.to_i)
          end
          response = {
            visualizations: query.build_paged(page, per_page).map { |v|
                VisualizationPresenter.new(v, current_viewer, self, { related: false }).with_presenter_cache(presenter_cache).to_poro
            },
            total_entries: vqb.build.count
          }
          if current_user
            # Prefetching at counts removes duplicates
            response.merge!({
              total_user_entries: VisualizationQueryBuilder.new.with_types(total_types).with_user_id(current_user.id).with_excluded_names(excludedNames).build.count,
              total_likes: VisualizationQueryBuilder.new.with_types(total_types).with_liked_by_user_id(current_user.id).with_excluded_names(excludedNames).build.count,
              total_shared: VisualizationQueryBuilder.new.with_types(total_types).with_shared_with_user_id(current_user.id).with_user_id_not(current_user.id).with_prefetch_table.with_excluded_names(excludedNames).build.count
            })
          end
        else
          # TODO: undesirable table hardcoding, needed for disambiguation. Look for
          # a better approach and/or move it to the query builder
          query = vqb.with_order("visualizations.#{order}", asc_order == 'true' ? :asc : :desc)
          if parent_category != -1
            query = query.with_parent_category(parent_category.to_i)
          end
          response = {
            visualizations: query.build_paged(page, per_page).map { |v|
                VisualizationPresenter.new(v, current_viewer, self, { related: false }).with_presenter_cache(presenter_cache).to_poro
            },
            total_entries: vqb.build.count
          }
          if current_user
            # Prefetching at counts removes duplicates
            response.merge!({
              total_user_entries: VisualizationQueryBuilder.new.with_types(total_types).with_user_id(current_user.id).build.count,
              total_likes: VisualizationQueryBuilder.new.with_types(total_types).with_liked_by_user_id(current_user.id).build.count,
              total_shared: VisualizationQueryBuilder.new.with_types(total_types).with_shared_with_user_id(current_user.id).with_user_id_not(current_user.id).with_prefetch_table.build.count
            })
          end
        end

        if response[:visualizations].empty? && name
          lib_datasets = common_data_user.visualizations.where(type: 'table', privacy: 'public', name: name).map do |v|
            VisualizationPresenter.new(v, current_viewer, self, { related: false })
              .with_presenter_cache(presenter_cache)
              .to_poro
          end
          lib_datasets.each do |dataset|
            dataset[:type] = 'remote'
            dataset[:needs_cd_import] = true
          end
          response[:visualizations] = lib_datasets
          response[:total_user_entries] = lib_datasets.count
        end
        
        render_jsonp(response)
      rescue CartoDB::BoundingBoxError => e
        render_jsonp({ error: e.message }, 400)
      rescue => e
        CartoDB::Logger.error(exception: e)
        render_jsonp({ error: e.message }, 500)
      end

      def likes_count
        render_jsonp({
          id: @visualization.id,
          likes: @visualization.likes.count
        })
      end

      def likes_list
        render_jsonp({
          id: @visualization.id,
          likes: @visualization.likes.map { |like| { actor_id: like.actor } }
        })
      end

      def is_liked
        render_jsonp({
          id: @visualization.id,
          likes: @visualization.likes.count,
          liked: current_viewer ? @visualization.is_liked_by_user_id?(current_viewer.id) : false
        })
      end

      def vizjson2
        render_vizjson(generate_vizjson2)
      end

      def vizjson3
        render_vizjson(generate_vizjson3(@visualization, params))
      end

      def list_watching
        return(head 403) unless @visualization.is_viewable_by_user?(current_user)
        watcher = CartoDB::Visualization::Watcher.new(current_user, @visualization)
        render_jsonp(watcher.list)
      end

      def static_map
        # Abusing here of .to_i fallback to 0 if not a proper integer
        map_width = params.fetch('width',nil).to_i
        map_height = params.fetch('height', nil).to_i

        # @see https://github.com/CartoDB/Windshaft-cartodb/blob/b59e0a00a04f822154c6d69acccabaf5c2fdf628/docs/Map-API.md#limits
        if map_width < 2 || map_height < 2 || map_width > 8192 || map_height > 8192
          return(head 400)
        end

        response.headers['X-Cache-Channel'] = "#{@visualization.varnish_key}:vizjson"
        response.headers['Surrogate-Key'] = "#{CartoDB::SURROGATE_NAMESPACE_VIZJSON} #{@visualization.surrogate_key}"
        response.headers['Cache-Control']   = "max-age=86400,must-revalidate, public"

        redirect_to Carto::StaticMapsURLHelper.new.url_for_static_map(request, @visualization, map_width, map_height)
      end

      def subcategories
        categoryId = params[:category_id]
        counts = params[:counts]

        if counts == 'true'
          categories = Sequel::Model.db.fetch("
            SELECT categories.*,
              (SELECT COUNT(*) FROM visualizations
                LEFT JOIN external_sources es ON es.visualization_id = visualizations.id
                LEFT JOIN external_data_imports edi
                  ON edi.external_source_id = es.id
                    AND (SELECT state FROM data_imports WHERE id = edi.data_import_id) <> 'failure'
                    AND edi.synchronization_id IS NOT NULL
            WHERE user_id=? AND
              edi.id IS NULL AND
              (type = 'table' OR type = 'remote') AND
              (category = categories.id OR category = ANY(get_viz_child_category_ids(categories.id)))) AS count FROM
                (SELECT id, name, parent_id, list_order FROM visualization_categories
                    WHERE id = ANY(get_viz_child_category_ids(?))
                    ORDER BY parent_id, list_order, name) AS categories;",
              current_user.id, categoryId
            ).all
        else
          categories = Sequel::Model.db.fetch("
            SELECT id, name, parent_id, list_order, -1 AS count FROM visualization_categories
              WHERE id = ANY(get_viz_child_category_ids(?))
              ORDER BY parent_id, list_order, name",
              categoryId
            ).all
        end

        render :json => categories.to_json
      end

      def search
        username = current_user.username
        query = params[:q]
        query.downcase!
        queryLike = '%' + query + '%'
        queryPrefix = query + ':*'
        queryPrefix.tr!(' ', '+')

        layers = Sequel::Model.db.fetch("
            SELECT id, username, type, name, description, tags, (1.0 / (CASE WHEN pos_name = 0 THEN 10000 ELSE pos_name END) + 1.0 / (CASE WHEN pos_tags = 0 THEN 100000 ELSE pos_tags END)) AS rank FROM (
              SELECT v.id, u.username, v.type, v.name, v.description, v.tags,
                COALESCE(position(? in lower(v.name)), 0) AS pos_name,
                COALESCE(position(? in lower(array_to_string(v.tags, ' '))), 0) * 1000 AS pos_tags
              FROM visualizations AS v
                  INNER JOIN users AS u ON u.id=v.user_id
                  LEFT JOIN external_sources AS es ON es.visualization_id = v.id
                  LEFT JOIN external_data_imports AS edi ON edi.external_source_id = es.id AND (SELECT state FROM data_imports WHERE id = edi.data_import_id) <> 'failure'
              WHERE edi.id IS NULL AND v.user_id=(SELECT id FROM users WHERE username=?) AND v.type IN ('table', 'remote') AND
              (
                to_tsvector(coalesce(v.name, '')) @@ to_tsquery(?)
                OR to_tsvector(array_to_string(v.tags, ' ')) @@ to_tsquery(?)
                OR v.name ILIKE ?
                OR array_to_string(v.tags, ' ') ILIKE ?
              )
            ) AS results
            ORDER BY rank DESC, type DESC LIMIT 50",
            query, query, username, queryPrefix, queryPrefix, queryLike, queryLike, query
          ).all

        if !current_user.has_feature_flag?('bbg_disabled_shared_empty_dataset') then
          emptyDatasetName = Cartodb.config[:shared_empty_dataset_name]

          layers.each_with_index do |layer, index|
            if layer[:name] == emptyDatasetName then
              layers.delete_at(index)
              break
            end
          end
        end

        render :json => '{"visualizations":' + layers.to_json + ' ,"total_entries":' + layers.size.to_s + '}'
      end

      def list
        user_id = current_user.id
        types = params.fetch(:types, Array.new).split(',')
        types.delete_if {|e| !LIST_VALID_VIS_TYPES.include? e }
        types = ['derived'] if types.empty?
        types.map!{ |e| "'" + e + "'" }
        typeList = types.join(",")
        only_liked = params.fetch(:only_liked, 'false') == 'true'
        only_locked = params.fetch(:locked, 'false') == 'true'
        tags = params.fetch(:tags, '').split(',')
        tags = nil if tags.empty?
        is_common_data_user = user_id == common_data_user.id

        args = [user_id, user_id]

        sharedEmptyDatasetCondition = is_common_data_user ? "" : "AND v.name <> '#{Cartodb.config[:shared_empty_dataset_name]}'"
        likedCondition = only_liked ? 'WHERE likes > 0' : ''
        lockedCondition = only_locked ? 'AND v.locked=true' : ''
        categoryCondition = ''
        parent_category = params.fetch(:parent_category, nil)
        if parent_category != nil
          categoryCondition = "AND (v.category = ? OR v.category = ANY(get_viz_child_category_ids(?)))"
          args += [parent_category, parent_category]
        end
        tagCondition = ''
        if tags
          tags.map! {|t| '%' + t.downcase + '%'}
          tags = '{' + tags.join(',') + '}'
          tagCondition = "AND (array_to_string(v.tags, ',') ILIKE ANY (?::text[]))"
          args += [tags]
        end

        union_common_data = !is_common_data_user && !((types.exclude? "'remote'") || only_liked || only_locked)

        if union_common_data
          if parent_category != nil
            args += [parent_category, parent_category]
          end
          if tags
            args += [tags]
          end
          args += [user_id]
        end

        order = params.fetch(:order, '')
        if !LIST_VALID_ORDER_COLS.include? order
          order = 'name'
        end

        orderDir = params.fetch(:asc_order, 'false') == 'true' ? 'ASC' : 'DESC'

        query = "
            SELECT * FROM (
              SELECT results.*, (SELECT COUNT(*) FROM likes WHERE actor=? AND subject=results.id) AS likes FROM (
                SELECT v.id, v.type, false AS needs_cd_import, v.name, v.display_name, v.description, v.tags, v.category, v.source, v.updated_at, v.locked, upper(v.privacy) AS privacy, ut.id AS table_id, ut.name_alias, edis.id IS NOT NULL AS from_external_source
                  FROM visualizations AS v
                      LEFT JOIN external_sources AS es ON es.visualization_id = v.id
                      LEFT JOIN visualizations AS v2 ON v2.user_id=v.user_id AND v.type='remote' AND v2.type='table' AND v2.name=v.name
                      LEFT JOIN user_tables AS ut ON ut.map_id=v.map_id
                      LEFT JOIN synchronizations AS s ON s.visualization_id = v.id
                      LEFT JOIN external_data_imports AS edis ON edis.synchronization_id = s.id
                  WHERE v2.id IS NULL AND v.user_id=? AND v.type IN (#{typeList}) #{lockedCondition} #{sharedEmptyDatasetCondition} #{categoryCondition} #{tagCondition}
                ) AS results
            ) AS results2
            #{likedCondition}"

        if union_common_data
          query += "UNION ALL
            SELECT v.id, 'remote' AS type, true AS needs_cd_import, v.name, v.display_name, v.description, v.tags, v.category, v.source, v.updated_at, v.locked, 'PUBLIC' AS privacy, ut.id AS table_id, ut.name_alias, true AS from_external_source, 0 AS likes
              FROM visualizations AS v
                LEFT JOIN user_tables AS ut ON ut.map_id=v.map_id
              WHERE v.user_id='#{common_data_user.id}' AND v.type='table' AND v.privacy='public' #{sharedEmptyDatasetCondition} #{categoryCondition} #{tagCondition} AND v.name NOT IN (SELECT name FROM visualizations WHERE user_id=? AND type IN (#{typeList}))"
        end

        query += " ORDER BY #{order} #{orderDir}"

        viz_list = Sequel::Model.db.fetch(query, *args).all

        render :json => '{"visualizations":' + viz_list.to_json + ' ,"total_entries":' + viz_list.count.to_s + '}'
      end

      def count
        user_id = current_user.id
        type = params.fetch(:type, 'datasets')
        typeList = (type == 'datasets') ? "'table','remote'" : "'derived'"
        categoryType = (type == 'datasets') ? 1 : 2

        is_common_data_user = user_id == common_data_user.id
        union_common_data = !is_common_data_user && (type == 'datasets')

        sharedEmptyDatasetCondition = is_common_data_user ? "" : "AND v.name <> '#{Cartodb.config[:shared_empty_dataset_name]}'"

        query = "SELECT
              COUNT(*) AS all,
              SUM(CASE WHEN likes > 0 THEN 1 ELSE 0 END) AS liked,
              SUM(CASE WHEN locked=true THEN 1 ELSE 0 END) AS locked,
              SUM(CASE WHEN type='table' THEN 1 ELSE 0 END) AS imported
            FROM (
              SELECT results.*, (SELECT COUNT(*) FROM likes WHERE actor=? AND subject=results.id) AS likes FROM (
                SELECT v.id, v.type, v.category, v.locked
                  FROM visualizations AS v
                      LEFT JOIN visualizations AS v2 ON v2.user_id=v.user_id AND v.type='remote' AND v2.type='table' AND v2.name=v.name
                  WHERE v2.id IS NULL AND v.user_id=? AND v.type IN (#{typeList}) #{sharedEmptyDatasetCondition}
                ) AS results"
        args = [user_id, user_id]

        if union_common_data
          query += " UNION ALL
                    SELECT v.id, 'remote' AS type, v.category, false AS locked, 0 AS likes
                      FROM visualizations AS v
                      WHERE v.user_id='#{common_data_user.id}' AND v.type='table' AND v.privacy='public' #{sharedEmptyDatasetCondition} AND v.name NOT IN (SELECT name FROM visualizations WHERE user_id=? AND type IN (#{typeList}))
                    "
          args += [user_id]
        end

        query += ") AS results2"
        type_counts = Sequel::Model.db.fetch(query, *args).all

        query = "SELECT categories.id, categories.parent_id, (
              (SELECT COUNT(*) FROM visualizations AS v
                  LEFT JOIN visualizations AS v2 ON v2.user_id=v.user_id AND v.type='remote' AND v2.type='table' AND v2.name=v.name
                WHERE v2.id IS NULL AND v.user_id=? AND v.type IN (#{typeList}) #{sharedEmptyDatasetCondition} AND v.category=categories.id) + "

        if union_common_data
          query += "(SELECT COUNT(*) FROM visualizations AS v
                WHERE v.user_id='#{common_data_user.id}' AND v.type='table' AND v.privacy='public' #{sharedEmptyDatasetCondition} AND v.category=categories.id AND
                  v.name NOT IN (SELECT name FROM visualizations WHERE user_id='#{user_id}' AND type IN (#{typeList})))"
        else
          query += "0"
        end

        query += ") AS count
            FROM (SELECT id, parent_id FROM visualization_categories WHERE type=#{categoryType}) AS categories"


        category_count_list = Sequel::Model.db.fetch(query, user_id).all

        category_counts = Hash.new

        category_count_list.each do |item|
          category_counts[item[:id]] = item[:count]
        end

        category_count_list.each do |item|
          parent_id = item[:parent_id]
          if category_counts.key?(parent_id)
            category_counts[parent_id] += item[:count]
          end
        end

        render :json => '{"types":' + type_counts[0].to_json + ' ,"categories":' + category_counts.to_json + '}'
      end



      private

      def generate_vizjson2
        Carto::Api::VizJSONPresenter.new(@visualization, $tables_metadata).to_vizjson(https_request: is_https?)
      end

      def render_vizjson(vizjson)
        set_vizjson_response_headers_for(@visualization)
        render_jsonp(vizjson)
      rescue KeyError => exception
        render(text: exception.message, status: 403)
      rescue CartoDB::NamedMapsWrapper::HTTPResponseError => exception
        CartoDB.notify_exception(exception, user: current_user, template_data: exception.template_data)
        render_jsonp({ errors: { named_maps_api: "Communication error with tiler API. HTTP Code: #{exception.message}" } }, 400)
      rescue CartoDB::NamedMapsWrapper::NamedMapDataError => exception
        CartoDB.notify_exception(exception)
        render_jsonp({ errors: { named_map: exception.message } }, 400)
      rescue CartoDB::NamedMapsWrapper::NamedMapsDataError => exception
        CartoDB.notify_exception(exception)
        render_jsonp({ errors: { named_maps: exception.message } }, 400)
      rescue => exception
        CartoDB.notify_exception(exception)
        raise exception
      end

      def load_by_name_or_id
        @table =  is_uuid?(@id) ? Carto::UserTable.where(id: @id).first  : nil

        # INFO: id should _really_ contain either an id of a user_table or a visualization, but for legacy reasons...
        if @table
          @visualization = @table.visualization
        else
          load_visualization
          @table = @visualization
        end
      end

      def load_visualization
        @visualization = load_visualization_from_id_or_name(params[:id])

        if @visualization.nil?
          raise Carto::LoadError.new('Visualization does not exist', 404)
        end
        if !@visualization.is_viewable_by_user?(current_viewer)
          raise Carto::LoadError.new('Visualization not viewable', 403)
        end
        unless request_username_matches_visualization_owner
          raise Carto::LoadError.new('Visualization of that user does not exist', 404)
        end
      end

      # This avoids crossing usernames and visualizations.
      # Remember that the url of a visualization shared with a user contains that user's username instead of owner's
      def request_username_matches_visualization_owner
        # Support both for username at `/u/username` and subdomain, prioritizing first
        username = [CartoDB.username_from_request(request), CartoDB.subdomain_from_request(request)].compact.first
        # URL must always contain username, either at subdomain or at path.
        # Domainless url documentation: http://cartodb.readthedocs.org/en/latest/configuration.html#domainless-urls
        return false unless username.present?

        # Either user is owner or is current and has permission
        # R permission check is based on current_viewer because current_user assumes you're viewing your subdomain
        username == @visualization.user.username ||
          (current_user && username == current_user.username && @visualization.has_read_permission?(current_viewer))
      end

      def id_and_schema_from_params
        if params.fetch('id', nil) =~ /\./
          @id, @schema = params.fetch('id').split('.').reverse
        else
          @id, @schema = [params.fetch('id', nil), nil]
        end
      end

      def set_vizjson_response_headers_for(visualization)
        # We don't cache non-public vis
        if @visualization.is_publically_accesible?
          response.headers['X-Cache-Channel'] = "#{@visualization.varnish_key}:vizjson"
          response.headers['Surrogate-Key'] = "#{CartoDB::SURROGATE_NAMESPACE_VIZJSON} #{visualization.surrogate_key}"
          response.headers['Cache-Control']   = 'no-cache,max-age=86400,must-revalidate, public'
        end
      end

      def to_json(visualization)
        ::JSON.dump(to_hash(visualization))
      end

      def to_hash(visualization)
        # TODO: previous controller uses public_fields_only option which I don't know if is still used
        VisualizationPresenter.new(visualization, current_viewer, self).to_poro
      end

    end
  end
end
