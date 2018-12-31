var ImportService = require('./imports/service_import/import_service_view');
var ImportTwitter = require('./imports/twitter_import/import_twitter_view');
var ImportDataView = require('./imports/import_data_view');
var ImportArcGISView = require('./imports/import_arcgis_view');

/**
 * Attributes:
 *
 *  className: import pane class view
 *  enabled: function that takes cdb.config and returns whether the service is enabled
 *  fallbackClassName: ...
 *  name: local name
 *  title: text for tab link
 *  options:
 *    - service:
 *    - fileExtensions:
 *    - showAvailableFormats:
 *    - acceptSync:
 *    - fileAttrs:
 *
 */

module.exports = {
  File: {
    className: ImportDataView,
    enabled: function (config, userData) { return true; },
    name: 'file',
    title: 'Data file',
    options: {
      type: 'url',
      fileEnabled: true,
      acceptSync: true
    }
  },
  Twitter: {
    className: ImportTwitter,
    enabled: function (config, userData) { return userData.twitter.enabled && !!config.get('datasource_search_twitter'); },
    fallback: 'common/views/create/listing/import_twitter_fallback',
    name: 'twitter',
    title: 'Twitter'
  }
};
