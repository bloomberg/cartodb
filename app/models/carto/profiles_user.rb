# encoding: UTF-8

module Carto
  class ProfilesUser < ActiveRecord::Base

    belongs_to :profile, foreign_key: :profile_id
    belongs_to :user, foreign_key: :user_id

  end
end
