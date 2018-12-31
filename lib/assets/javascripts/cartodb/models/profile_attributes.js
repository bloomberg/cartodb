cdb.admin.ProfileAttributes = cdb.core.Model.extend({

  /**
   * Do nothing. The ProfileAttributes model is read-only.
   */
  save: function () {
    console.log("ProfileAttributes.save unsupported - ProfileAttributes are read-only.");
  },

  /**
   * Return url for fetching this model. methods other than
   * GET are unsupported.
   * @param method http request method
   */
  url: function (method) {
    return "/api/v1/profile_attributes";
  },

}, {

  /**
   * Static utility for loading profile attributes.
   * If user, a cdb.admin.User model, is specified,
   * it will load an instance of ProfileAttributes
   * from the user, otherwise it will instantiate a
   * new user from user_Data and pull its' profile
   * attributes.
   *
   * @param user nullable
   * @return cdb.admin.ProfileAttributes instance
   *         representing profile data for the user
   */
  load: function (user = null) {
    return (user || new cdb.admin.User(user_data)).profileAttributes;
  },

});
