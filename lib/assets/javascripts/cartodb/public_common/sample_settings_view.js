var cdb = require('cartodb.js-v3');
var SettingsDropdown = require('./user_settings/dropdown_view');
var $ = require('jquery-cdb-v3');
var CreateMapModel = require('../common/dialogs/create/create_map_model');
var ViewFactory = require('../common/view_factory');
var randomQuote = require('../common/view_helpers/random_quote');
var ExportMapModel = require('../models/export_map_model');


/**
 * View to render the user settings section in the header.
 * Expected to be created from existing DOM element.
 */
module.exports = cdb.core.View.extend({

  events: {
    'click .js-dropdown-target': '_createDropdown',
    'click .js-save-map': '_sampleMapSave'
  },

  initialize : function(opts) {
    this.currentUser = opts.currentUser;
    this.description = opts.description;
    this.vizName = opts.vizName;
    this.vizId = opts.vizId;
  },

  render: function() {
    var dashboardUrl = this.model.viewUrl().dashboard();
    var datasetsUrl = dashboardUrl.datasets();
    var mapsUrl = dashboardUrl.maps();

    this.$el.html(
      cdb.templates.getTemplate('public_common/sample_settings_template')({
        avatarUrl: this.model.get('avatar_url'),
        mapsUrl: mapsUrl,
        datasetsUrl: datasetsUrl,
        vis_name: window.vis_name,
        dashboard_url: dashboardUrl.toString()
      })
    );

    return this;
  },

  _getDataSet: function(name, layers) {
        var collection = new cdb.admin.Visualizations();

        collection.options.set({
          name: name,
          tags: '',
          shared: false,
          locked: false,
          only_liked: false,
          samples: false,
          order: 'updated_at',
          types: 'table,remote',
          type: '',
          deepInsights: false,
          per_page: 1000 // Increase this number, if the datalibrary datasets go beyond this number
        });

        collection.bind('loaded', function datasetsLoadedCallback(that) {
          collection.unbind('loaded', datasetsLoadedCallback);

          var ds = collection.where({ name: name });
          layers.push(ds[0]);
        });

      return collection.fetch();
  },

  _sampleMapSave: function() {
    if(this.currentUser.featureEnabled('bbg_rtr_disable_sample_save_as')) {
      this._saveMap();
    }
    else {
      this._saveAsMap();
    }
  },

  _endPollCallback: function( loadingView, state ) {
    if(state === 'complete') {
      var vis = new cdb.admin.Visualization({ id: this._importModel.get('visualization_id') });
      var vis_url = this.currentUser.viewUrl() + '/' + vis.viewUrl(this.currentUser).edit();

      loadingView.close();
      window.location = vis_url
    }
    else {
      var errorView = ViewFactory.createDialogByTemplate('common/templates/fail', {
        msg: 'Failed saving map to current user'
      });
      loadingView.close();
      errorView.appendToBody();
    }
  },

  _saveAsMap: function() {
        // loading dialog box
        var loadingView = ViewFactory.createDialogByTemplate('common/templates/loading', {
          title: 'Saving map ...',
          quote: randomQuote()
        });

        loadingView.appendToBody();
    
        this._exportMapModel = new cdb.admin.ExportMapModel({
                visualization_id: this.vizId
              });
        this._exportMapModel.requestExport();

        this._exportMapModel.bind('change:state', function () {
          var state = this._exportMapModel.get('state');
          if (state === 'complete') {
            // Download the file and trigger the import
            url = this._exportMapModel.get('url')

            var ImportModel = require('../common/background_polling/models/import_model');
            this._importModel = new ImportModel({
              endPollCallback: this._endPollCallback.bind(this, loadingView)
            });

            var urlRegex = new RegExp('^https?:\/\/');
            if(!urlRegex.test(url)) {
              var origBaseUrl = this.model.viewUrl().get('base_url')
              var newBaseUrl = origBaseUrl.slice(0, origBaseUrl.indexOf("user")-1);
              url = encodeURI(newBaseUrl + url)
            }
            this._importModel.createImport({
              type: 'url',
              create_vis: true,
              value: url
            });

            //loadingView.close();
          } else if (state === 'failure') {
            var errorView = ViewFactory.createDialogByTemplate('common/templates/fail', {
              msg: 'Failed saving map'
            });
            loadingView.close();
            errorView.appendToBody();
          }
        }, this);

  },

  _saveMap: function() {

        var loadingView = ViewFactory.createDialogByTemplate('common/templates/loading', {
          title: 'Saving map ...',
          quote: randomQuote()
        });

        var currentUser = this.currentUser;
        var vizName = this.vizName;
        var description = this.description;
   
        loadingView.appendToBody();

        var datasets = window.datalib_layers;
        var layers = [];
        var promises = [];

        for(var i=0; i<datasets.length; ++i) {
          promises.push(this._getDataSet(datasets[i], layers));
        }

        //Now, go through all the deferred promises :-)
        $.when.apply($, promises).done(function() {

          var d = {
              type: 'map',
              listing: 'import',
              selectedItems: layers
            }
   
          var CreateMapModel = require('../common/dialogs/create/create_map_model');
          

          var createModel;
          d = d || {};
          if (d.type === 'dataset') {
            createModel = new CreateDatasetModel({}, {
              user: currentUser
            });
          } 
          else {
            var options = {};
            if (d.listing !== undefined) {
              options.listing = d.listing;
          }

          var viz_descriptionTag = description;
          var xmlParser = new DOMParser();
          xmlDoc = xmlParser.parseFromString(viz_descriptionTag, "text/xml");
          var viz_description = "";
          if (xmlDoc.getElementsByTagName("p").length > 0) {
            viz_description = xmlDoc.getElementsByTagName("p")[0].childNodes[0].nodeValue;
          }

          createModel = new CreateMapModel(
            options, _.extend({user: currentUser,
                               mapName: vizName + " (Copy)",
                               description: viz_description
                                },d) );
        }

        createModel.bind('datasetCreated', function(tableMetadata) {
          if (router.model.isDatasets()) {
            var vis = new cdb.admin.Visualization({ type: 'table' });
            vis.permission.owner = currentUser;
            vis.set('table', tableMetadata.toJSON());
            window.location = vis.viewUrl(currentUser).edit();
          } else {
            var vis = new cdb.admin.Visualization({ name: DEFAULT_VIS_NAME });
            vis.save({
              tables: [ tableMetadata.get('id') ]
            },{
              success: function(m) {
                window.location = vis.viewUrl(currentUser).edit();
              },
              error: function(e) {
                loadingView.close();
                collection.trigger('error');
              }
            });
          }
        }, this);

        createModel.viewsReady();


      });


  },

  _openCreateDialog : function(d, loadingView) {
        var CreateMapModel = require('../common/dialogs/create/create_map_model');

        var createModel;
        d = d || {};
        if (d.type === 'dataset') {
          createModel = new CreateDatasetModel({}, {
            user: currentUser
          });
        } else {
          var options = {};
          if (d.listing !== undefined) {
            options.listing = d.listing;
          }

          var viz_descriptionTag = window.vizdata.description;
          var xmlParser = new DOMParser();
          xmlDoc = xmlParser.parseFromString(viz_descriptionTag, "text/xml");
          var viz_description = "";
          if (xmlDoc.getElementsByTagName("p").length > 0) {
            viz_description = xmlDoc.getElementsByTagName("p")[0].childNodes[0].nodeValue;
          }

          createModel = new CreateMapModel(
            options, _.extend({user: currentUser,
                               mapName: window.vis_name,
                               description: viz_description
                                },d) );
        }


        createModel.bind('datasetCreated', function(tableMetadata) {
          if (router.model.isDatasets()) {
            var vis = new cdb.admin.Visualization({ type: 'table' });
            vis.permission.owner = currentUser;
            vis.set('table', tableMetadata.toJSON());
            window.location = vis.viewUrl(currentUser).edit();
          } else {
            var vis = new cdb.admin.Visualization({ name: DEFAULT_VIS_NAME });
            vis.save({
              tables: [ tableMetadata.get('id') ]
            },{
              success: function(m) {
                window.location = vis.viewUrl(currentUser).edit();
              },
              error: function(e) {
                loadingView.close();
                collection.trigger('error');
              }
            });
          }
        }, this);

      createModel.viewsReady();
    },


  _createDropdown: function(ev) {
    this.killEvent(ev);
    cdb.god.trigger('closeDialogs');

    var view = new SettingsDropdown({
      target: $(ev.target),
      model: this.model, // user
      horizontal_offset: 18
    });
    view.render();

    view.on('onDropdownHidden', function() {
      view.clean();
    }, this);

    view.open();
  }

});
