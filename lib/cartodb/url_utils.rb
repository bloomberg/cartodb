require 'uri'
require 'cgi'

module CartoDB
  module UrlUtils

    # Return GET parameters for a url as a Hash
    # of parameter names as strings to Arrays of
    # values.  Note that the return value for a
    # nonexistent parameter name is an empty Array
    # as opposed to `Nil`.
    # @param url String
    # @return Hash
    def get_url_params(url)
      CGI.parse(URI.parse(url).query)
    end
    module_function :get_url_params

    # Return values of a GET paramter identified
    # by the specified param_name, or an empty
    # array if no values for the param_name are
    # specified.
    # @param url String
    # @param param_name String
    # @return Array of String
    def get_url_param(url, param_name)
      get_url_params(url)[param_name]
    end
    module_function :get_url_param

  end
end