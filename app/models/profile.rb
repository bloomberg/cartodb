# coding: UTF-8

class Profile < Sequel::Model
  include CartoDB::MiniSequel

  one_to_many :profiles_user

  def attrs_hash
    attrs ? JSON.parse(attrs) : nil
  end
end

