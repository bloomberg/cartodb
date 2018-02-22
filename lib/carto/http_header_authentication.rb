# encoding: utf-8
require_dependency 'carto/uuidhelper'

module Carto
  class HttpHeaderAuthentication
    include Carto::UUIDHelper

    def valid?(request)
      value = header_value(request.headers)
      puts "user-auto-creation : header authetication is valid ? #{value}"
      !value.nil? && !value.empty?
    end

    def get_user(request)
      puts "header-auth : getting user"
      header = identity(request)
      return nil if header.nil? || header.empty?

      puts "header-auth : valid header"

      puts "header-auth : checking profiles header"

      user = ::User.where("#{field(request)} = ?", header).first
      parse_profiles_header(user, request.headers)
      user
    end

    def autocreation_enabled?
      puts "user-auto-creation : autocreation_enabled"
      Cartodb.get_config(:http_header_authentication, 'autocreation') == true
    end

    def autocreation_valid?(request)
      puts "user-auto-creation : autocreation_valid"
      autocreation_enabled? && field(request) == 'username'
      #autocreation_enabled? && field(request) == 'email'
    end

    def identity(request)
      header_value(request.headers)
    end

    def email(request)
      raise "Configuration is not set to email, or it's auto but request hasn't email" unless field(request) == 'email'
      identity(request)
    end

    def creation_in_progress?(request)
      puts "user-auto-creation : inside creation_in_progress"
      header = identity(request)
      return false unless header
      puts "user-auto-creation : header exists"

      Carto::UserCreation.in_progress.where("#{user_creation_field(request)} = ?", header).first.present?
    end

    private

    def field(request)
      field = Cartodb.get_config(:http_header_authentication, 'field')
      puts "user-auto-creation : The http headers field is #{field}" 
      field == 'auto' ? field_from_value(request) : field
    end

    def user_creation_field(request)
      field = field(request)
      case field
      when 'username', 'email'
        field
      when 'id'
        'user_id'
      else
        raise "Unknown field #{field}"
      end
    end

    def field_from_value(request)
      value = header_value(request.headers)
      return nil unless value

      if value.include?('@')
        'email'
      elsif is_uuid?(value)
        'id'
      else
        'username'
      end
    end

    def header_value(headers)
      header = ::Cartodb.get_config(:http_header_authentication, 'header')
      puts "user-auto-creation : Trying to extract value from headers for #{header}, value is #{headers[header || '']}"
      !header.nil? && !header.empty? ? headers[header] : nil
    end

    def get_profiles_header_field
      @profiles_header_field ||= ::Cartodb.get_config(:http_header_authentication, 'profiles_header_field')
    end

    def get_profiles_header_null_value
      @profiles_header_null_value ||= ::Cartodb.get_config(:http_header_authentication, 'profiles_header_null_value')
    end

    def parse_profiles_header(user, headers)
      if user
        profiles_header_field = get_profiles_header_field
        profiles_header_null_value = get_profiles_header_null_value
        profiles_header_value = nil
        if (profiles_header_field &&
            profiles_header_null_value &&
            (profiles_header_value = headers[profiles_header_field]))
          profile_names = profiles_header_value == profiles_header_null_value ? 
                            [] : profiles_header_value.split(',')
          puts "header-auth : parse_profiles_header - setting profiles for user #{user.username} to #{profile_names.to_s}"
          user.replace_session_profiles(profile_names)
        end
      end
    end

  end
end
