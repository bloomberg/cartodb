require 'uri'
require 'cgi'

module Carto
  class Url

    # @param String representing any valid uri
    def initialize(url_string)
      @uri = URI.parse(url_string)
    end

    # Return GET parameters for a url as a Hash
    # of parameter names as strings to Arrays of
    # values.  Note that the return value for a
    # nonexistent parameter name is an empty Array
    # as opposed to `Nil`. The returned Hash supports
    # access by Symbols or Strings interchangeably.
    # @return Hash
    def get_params
      # Make a copy of local params
      configured_params(get_params_local)
    end

    # Return values of a GET paramter identified
    # by the specified param_name, or an empty
    # array if no values for the param_name are
    # specified.
    # @param url String
    # @param param_name String
    # @return Array of String
    def get_param(param_name)
      get_params_local[param_name]
    end

    private

    # Return a Hash for which the default value is an array
    def get_params_local
      @cgi ||= configured_params(@uri.query ? CGI.parse(@uri.query) : {})
    end

    # Return a copy of params with indifferent
    # access and empty array default
    # @param params Hash
    # @return Hash
    def configured_params(params)
      p = params.with_indifferent_access
      p.default_proc = lambda { |h, k| [] }
      p
    end

  end
end
