require 'active_record'

module Carto
  class Category < ActiveRecord::Base
    self.inheritance_column = :_type_disabled
    set_table_name "visualization_categories"
    has_many :visualization, class_name: Carto::Visualization
  end
end
