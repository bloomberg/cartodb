module.exports = function (attrs) {
  var original = attrs['original'];
  var alias = attrs['alias'];
  var user = attrs['user'];
  var canSeeOriginals = user && user.featureEnabled('aliases');

  if (canSeeOriginals && alias && alias !== original) {
    var aliasedName = alias + ' (' + original + ')';
  } else {
    var aliasedName = alias || original;
  }

  return aliasedName;
}
