# encoding: UTF-8

module Carto
  class Profile < ActiveRecord::Base

    validates :name, presence: true

    has_many :profiles_users, dependent: :destroy

    def attrs_hash
      attrs ? JSON.parse(attrs) : nil
    end

  end
end
