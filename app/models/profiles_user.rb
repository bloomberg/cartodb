# encoding: UTF-8

class ProfilesUser < Sequel::Model
  include CartoDB::MiniSequel

  many_to_one :profile
  many_to_one :user

end

