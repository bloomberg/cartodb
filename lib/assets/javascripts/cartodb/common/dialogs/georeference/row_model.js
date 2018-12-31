var cdb = require('cartodb.js-v3');
var RowView = require('./row_view');

/**
 * Model for an individual row
 */
module.exports = cdb.core.Model.extend({

  defaults: {
    comboViewClass: 'CustomTextCombo',
    label: '',
    placeholder: 'Select column or enter value',
    isFreeText: false,
    data: []
  },

  _initViewBinds: function(view) {
    view.bind('change:value', function() {
      this.set('value', view.get('value'));
    }, this);
  },

  createView: function() {
    var view = new RowView({
      model: this
    });
    this._initViewBinds(view);
    return view;
  }

});
