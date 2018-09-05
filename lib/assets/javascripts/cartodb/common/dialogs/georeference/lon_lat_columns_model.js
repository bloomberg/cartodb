var cdb = require('cartodb.js-v3');
var Backbone = require('backbone-cdb-v3');
var RowModel = require('./row_model');
var RowsView = require('./rows_view');
var DefaultFooterView = require('./default_footer_view');
var ViewFactory = require('../../view_factory');

/**
 * Model for the Lon/Lat georeference option.
 */
module.exports = cdb.core.Model.extend({

  TAB_LABEL: 'Latitude/Longitude',

  defaults: {
    columnsNames: []
  },

  initialize: function(attrs) {
    if (!attrs.geocodeStuff) throw new Error('geocodeStuff is required');
    if (!attrs.columnsNames) throw new Error('columnsNames is required');
  },

  createView: function() {
    this.set({
      canContinue: false,
      hideFooter: false
    });
    this._initRows();

    var titleHtml =
      'Select the columns containing latitude and longitude for <b>' +
      this.get('geocodeStuff').get('tableName') + '</b> to map data';

    return ViewFactory.createByList([
      ViewFactory.createByTemplate('common/dialogs/georeference/default_content_header', {
        titleHtml,
        desc: ''
      }),
      new RowsView({
        model: this
      }),
      new DefaultFooterView({
        model: this
      })
    ]);
  },

  assertIfCanContinue: function() {
    var canContinue = this.get('rows').all(function(m) {
      return !!m.get('value');
    });
    this.set('canContinue', canContinue);
  },

  continue: function() {
    var firstValue = this.get('rows').first().get('value');
    var lastValue = this.get('rows').last().get('value');
    var columnAliases = this.get('columnAliases');

    var d = this.get('geocodeStuff').geocodingChosenData({
      type: 'lonlat',
      longitude: columnAliases[firstValue] || firstValue,
      latitude: columnAliases[lastValue] || lastValue
    });

    this.set('geocodeData', d);
  },

  _initRows: function() {
    var rows = new Backbone.Collection([
      new RowModel({
        comboViewClass: 'Combo',
        label: 'Latitude',
        placeholder: 'Select column',
        property: 'latitude',
        data: this.get('columnsNames')
      }),
      new RowModel({
        comboViewClass: 'Combo',
        label: 'Longitude',
        placeholder: 'Select column',
        property: 'longitude',
        data: this.get('columnsNames')
      }),
    ]);
    this.set('rows', rows);
  }
});
