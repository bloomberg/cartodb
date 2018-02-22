
class Api::Json::ProfileAttributesController < Api::ApplicationController

  def show
    render json: current_user.nil? ? {} : current_user.profile_attributes
  end

end
