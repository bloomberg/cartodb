var _ = require('underscore-cdb-v3');
var Backbone = require('backbone-cdb-v3');
var moment = require('moment-v3');
var cdb = require('cartodb.js-v3');
var StreetAddressesView = require('./street_addresses_view');
var RowModel = require('../row_model');
var StreetRowModel = require('./street_row_model');

/**
 * Model for the street addresses georeference option.
 */
module.exports = cdb.core.Model.extend({

  TAB_LABEL: 'Street Addresses',
  MAX_STREET_ROWS: 3,

  defaults: {
    columnsNames: [],
    columns: [],
    estimation: undefined
  },

  initialize: function(attrs) {
    if (!attrs.geocodeStuff) throw new Error('geocodeStuff is required');
    if (!_.isBoolean(attrs.isGoogleMapsUser)) throw new Error('isGoogleMapsUser is required');
    if (!attrs.userGeocoding) throw new Error('userGeocoding is required');
    if (!attrs.columns) throw new Error('columns is required');
    if (!attrs.estimation) throw new Error('estimation is required'); // cdb.admin.Geocodings.Estimations
  },

  createView: function() {
    this.set({
      canContinue: false,
      hideFooter: false,
      mustAgreeToTOS: false,
      confirmTOS: false,
      hasAgreedToTOS: false
    });
    this._initRows();

    return new StreetAddressesView({
      model: this
    });
  },

  isDisabled: function() {
    return !this.get('isGoogleMapsUser') && this.get('userGeocoding').hasReachedMonthlyQuota();
  },

  showCostsInfo: function() {
    return !this.get('isGoogleMapsUser');
  },

  getFormatterItemByRow: function(m) {
    var value = m.get('value');
    var val = this.get('columnAliases')[value] || value;
    if (val) {
      return m.get('isFreeText') ? val.trim() : '{' + val + '}';
    }
  },

  assertIfCanAddMoreRows: function() {
    // If can add more rows, enable the add button only on the last street row
    var streetRows = this.get('rows').filter(this._isStreetRow);
    _.invoke(streetRows, 'set', 'canAddRow', false);
    _.last(streetRows).set('canAddRow', streetRows.length < this.MAX_STREET_ROWS);
  },

  daysLeftToNextBilling: function() {
    var today = moment().startOf('day');
    return moment(this.get('lastBillingDate')).add(30, 'days').diff(today, 'days')
  },

  continue: function() {
    var mustAgreeToTOS = this.get('mustAgreeToTOS');

    if (this._hasAgreedToTOS() || !mustAgreeToTOS) {
      var d = this.get('geocodeStuff').geocodingChosenData({
        type: 'address',
        kind: 'high-resolution',
        formatter: this.get('formatter')
      });
      this.set('geocodeData', d);
    } else if (mustAgreeToTOS) {
      this.set('confirmTOS', true);
    }
  },

  hasHardLimit: function() {
    return !!this.get('userGeocoding').get('hard_limit');
  },

  _hasAgreedToTOS: function() {
    return this.get('mustAgreeToTOS') && this.get('hasAgreedToTOS');
  },

  _isStreetRow: function(row) {
    return row instanceof StreetRowModel;
  },

  _initRows: function() {
    var columns = this.get('columns');
    var isDisabled = this.isDisabled();
    var canAddRow =
      _(user_data.feature_flags).contains('georeference_advanced_features');

    var addressRow = new StreetRowModel({
          label: 'Address',
          data: columns,
          canAddRow: canAddRow,
          disabled: isDisabled
        }),
        countryRow = new RowModel({
          label: 'Country',
          data: columns,
          disabled: true
        }),
        stateRow = new RowModel({
          label: 'State/Province',
          data: columns,
          disabled: true
        });

    // Enable country and state conditionally upon selection
    // of address and country, respectively.
    addressRow.bind('change:value', function() {
      countryRow.set('disabled', !addressRow.get('value'));
    });
    countryRow.bind('change:value', function() {
      stateRow.set('disabled', !countryRow.get('value'));
    });

    var rows = new Backbone.Collection([
      addressRow,
      countryRow,
      stateRow,
    ]);
    this.set('rows', rows);
  }

});
