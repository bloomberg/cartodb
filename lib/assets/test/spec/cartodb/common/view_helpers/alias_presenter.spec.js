var presentAlias = require('../../../../../javascripts/cartodb/common/view_helpers/alias_presenter');

describe('common/view_helpers/alias_presenter', function() {
  var original = 'original';
  var alias = 'alias';
  var _createMockUser = function (options) {
    var user = {};

    user.featureEnabled = function (feature) {
      return (feature === 'aliases' && options['aliasCapable']) ? true : false;
    };

    return user;
  };

  describe('when user is null', function () {
    var aliasedName = presentAlias({ original: original, alias: alias });

    it('should not show original names', function () {
      expect(aliasedName).toContain(alias);
      expect(aliasedName).not.toContain(original);
      expect(aliasedName).toEqual(alias);
    });

    it('should not be empty', function () {
      expect(aliasedName).not.toEqual('');
    });
  });

  describe('when user is alias capable', function () {
    var user = _createMockUser({ aliasCapable: true });

    describe('and alias name is present', function () {
      it('should be formatted like: aliased_name (original_name)', function () {
        var aliasedName = presentAlias({ user: user, original: original, alias: alias });

        expect(aliasedName).toContain(original);
        expect(aliasedName).toContain(alias);
        expect(aliasedName).toEqual(alias + ' (' + original + ')');
      });

      it('should ignore alias if it is the same as original', function () {
        var aliasedName = presentAlias({ user: user, original: original, alias: original });

        expect(aliasedName).toEqual(original);
      });
    });

    describe('and alias name is not present', function () {
      it('should only show original name', function () {
        var aliasedName = presentAlias({ user: user, original: original });

        expect(aliasedName).not.toContain(alias);
        expect(aliasedName).toEqual(original);
      });
    });
  });

  describe('when user is not alias capable', function () {
    var user = _createMockUser({ aliasCapable: false });

    describe('and alias name is present', function () {
      it('should only show aliased name', function () {
        var aliasedName = presentAlias({ user: user, original: original, alias: alias });

        expect(aliasedName).not.toContain(original);
        expect(aliasedName).toContain(alias);
        expect(aliasedName).toEqual(alias);
      });
    });

    describe('and alias name is not present', function () {
      it('should only show original name', function () {
        var aliasedName = presentAlias({ user: user, original: original });

        expect(aliasedName).not.toContain(alias);
        expect(aliasedName).toEqual(original);
      });
    });
  });
});
