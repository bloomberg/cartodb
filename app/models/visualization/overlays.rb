# encoding: utf-8
module CartoDB
  module Visualization
    class Overlays

      def initialize(visualization)
        @visualization = visualization
      end

      def create_default_overlays
        prof_attrs = @visualization.user && @visualization.user.profile_attributes
        create_share_overlay(@visualization, 2)
        if @visualization.user.has_feature_flag?('bbg_pro_ui')
          create_search_overlay(@visualization, 3)
        end
        create_zoom_overlay(@visualization, 6)
        create_loader_overlay(@visualization, 8)

        # nil check added to support feature flag check (see #6108) without breaking backguards compatibility
        if @visualization.user.nil? || !@visualization.user.has_feature_flag?('disabled_cartodb_logo')
          create_logo_overlay(@visualization, 9)
        end

        if prof_attrs && prof_attrs["defaultFixedTitleOn"]
          create_title_overlay(@visualization, 1, prof_attrs)
        end
      end

      private

      def create_logo_overlay(member, order)
        options = { display: true, x: 10, y: 40 }

        Carto::Overlay.new(
          order: order,
          type: "logo",
          template: '',
          options: options,
          visualization_id: member.id
        ).save
      end

      def generate_overlay(id, options, type, order)
        Carto::Overlay.new(
          order: order,
          type: type,
          template: "",
          options: options,
          visualization_id: id
        )
      end

      def create_loader_overlay(member, order)
        options = { display: true, x: 20, y: 150 }

        Carto::Overlay.new(
          order: order,
          type: "loader",
          template: '<div class="loader" original-title=""></div>',
          options: options,
          visualization_id: member.id
        ).save
      end

      def create_zoom_overlay(member, order)
        options = { display: true, x: 20, y: 20 }

        Carto::Overlay.new(
          order: order,
          type: "zoom",
          template: '<a href="#zoom_in" class="zoom_in">+</a> <a href="#zoom_out" class="zoom_out">-</a>',
          options: options,
          visualization_id: member.id
        ).save
      end

      def create_share_overlay(member, order)
        options = { display: true, x: 20, y: 20 }

        generate_overlay(member.id, options, "share", order).save
      end

      def create_search_overlay(member, order)
        options = { display: true, x: 60, y: 20 }

        generate_overlay(member.id, options, "search", order).save
      end

      def create_title_overlay(member, order, prof_attrs = {})
        options = {
          "x": 0,
          "y": 0,
          "display": true,
          "style": {
            "z-index": 4,
            "color": "#ffffff",
            "text-align": "left",
            "font-size": prof_attrs["defaultFixedTitleFontSize"] || 20,
            "font-family-name": prof_attrs["defaultFontFamilyName"] || "Helvetica",
            "box-padding": 10,
            "box-color": "#000000",
            "box-opacity": prof_attrs["defaultFixedTitleOpacity"] || 0.7
          },
          "extra": {
            "headerType": "title",
            "text": member.name,
            "rendered_text": member.name
          }
        }

        generate_overlay(member.id, options, "header", order).save
      end

    end
  end
end
