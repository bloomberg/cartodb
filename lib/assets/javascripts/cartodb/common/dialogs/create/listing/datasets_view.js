var cdb = require('cartodb.js-v3');
var _ = require('underscore-cdb-v3');
var ListView = require('../../../../dashboard/list_view');
var DatasetsList = require('../../../../dashboard/list_view');
var ContentResult = require('./datasets/content_result_view');
var DatasetsPaginator = require('./datasets/datasets_paginator_view');
var SideMenuView = require('../../../../dashboard/side_menu_view');

/**
 *  Datasets list view
 *
 *  Show datasets view to select them for
 *  creating a map or importing a dataset
 *
 */

module.exports = cdb.core.View.extend({

  initialize: function() {
    this.user = this.options.user;
    this.createModel = this.options.createModel;
    this.routerModel = this.options.routerModel;

    this._preRender();
    this._initViews();
    this._initBindings();

    this._onRouterChange(); // needed to show loading animation initially
  },

  _initBindings: function() {
    this.routerModel.bind('change', this._onRouterChange, this);
    this.collection.bind('loading', this._onDataLoading, this);
    this.collection.bind('reset', this._onDataFetched, this);
    this.collection.bind('error', function(e) {
      // Old requests can be stopped, so aborted requests are not
      // considered as an error
      if (!e || (e && e.statusText !== "abort")) {
        this._onDataError()
      }
    }, this);
    this.add_related_model(this.routerModel);
    this.add_related_model(this.createModel);
    this.add_related_model(this.collection);
  },

  _preRender: function() {
    this.$el.html('<div class="ContentController">\
        <div class="LeftMenu"></div>\
        <div class="RightContent">\
          <div class="SearchBar"><input type="search" class="search-bar-input" placeholder="<Filter>"></div>\
          <div class="NoDatasets"></div>\
          <div class="ContentList" id="content-list"></div>\
        </div>\
      </div>');
  },

  _initViews: function() {
    this.controlledViews = {};  // All available views
    this.enabledViews = [];     // Visible views

    var childContainer = this.$el.find('.RightContent');

    var noDatasetsView = new ContentResult({
      className:  'ContentResult no-datasets',
      user: this.user,
      defaultUrl: this.options.defaultUrl,
      routerModel: this.routerModel,
      collection: this.collection,
      template: 'common/views/create/listing/content_no_datasets'
    });
    noDatasetsView.bind('connectDataset', function() {
      if (this.user.canCreateDatasets()) {
        this.createModel.set('listing', 'import');
      }
    }, this);
    noDatasetsView.render().hide();
    this.controlledViews.no_datasets = noDatasetsView;
    childContainer.append(noDatasetsView.el);
    this.addView(noDatasetsView);

    var sideMenuView = new SideMenuView({
      el:           this.$('.LeftMenu'),
      user:         this.user,
      routerModel:  this.routerModel,
      collection:   this.collection
    });
    this.controlledViews.sideMenu = sideMenuView;
    sideMenuView.render();
    this.addView(sideMenuView);

    var listView = undefined;
    if (typeof DatasetsList === "function") {
      listView = new DatasetsList({
        user:         this.user,
        createModel:  this.createModel,
        routerModel:  this.routerModel,
        collection:   this.collection,
        parentEl:     this.$('#content-list')
      });
    } else {
      listView = new window.ListView({
        user:         this.user,
        createModel:  this.createModel,
        routerModel:  this.routerModel,
        collection:   this.collection,
        parentEl:     this.$('#content-list')
      });
    }

    this.controlledViews.list = listView;
    this.$('#content-list').append(listView.render().el);
    this.addView(listView);

    var updateCounts = function() {
      setTimeout(function(){
        sideMenuView.updateCounts();
      }, 300);
    };

    listView.bind('vis-states-changed', function() {
      updateCounts();
    });

    var noResultsView = new ContentResult({
      defaultUrl: this.options.defaultUrl,
      routerModel: this.routerModel,
      collection: this.collection,
      template: 'common/views/create/listing/datasets_no_result'
    });
    noResultsView.render().hide();
    this.controlledViews.no_results = noResultsView;
    childContainer.append(noResultsView.el);
    this.addView(noResultsView);

    var errorView = new ContentResult({
      defaultUrl: this.options.defaultUrl,
      routerModel: this.routerModel,
      collection: this.collection,
      template: 'common/views/create/listing/datasets_error'
    });
    errorView.render().hide();
    this.controlledViews.error = errorView;
    childContainer.append(errorView.el);
    this.addView(errorView);

    var mainLoaderView = new ContentResult({
      defaultUrl: this.options.defaultUrl,
      routerModel: this.routerModel,
      collection: this.collection,
      template: 'common/views/create/listing/datasets_loader'
    });

    this.controlledViews.main_loader = mainLoaderView;
    childContainer.append(mainLoaderView.render().el);
    this.addView(mainLoaderView);
  },

  _onRouterChange: function() {
    this._hideBlocks();
    this._showBlocks(['sideMenu', 'main_loader']);
  },

  /**
   * Arguments may vary, depending on if it's the collection or a model that triggers the event callback.
   * @private
   */
  _onDataFetched: function() {
    var activeViews = ['sideMenu'];
    var tag = this.routerModel.get('tag');
    var q = this.routerModel.get('q');
    var shared = this.routerModel.get('shared');
    var locked = this.routerModel.get('locked');
    var library = this.routerModel.get('library');

    if (this.collection.size() === 0) {
      activeViews.push('no_results');
    } else {
      activeViews.push('list');
    }

    this._hideBlocks();
    this._showBlocks(activeViews);
  },

  _onDataLoading: function() {
    this._hideBlocks();
    this._showBlocks(['sideMenu', 'main_loader']);
  },

  _onDataError: function(e) {
    this._hideBlocks();
    this._showBlocks(['sideMenu', 'error']);
  },

  _showBlocks: function(views) {
    var self = this;
    if (views) {
      _.each(views, function(v){
        if (self.controlledViews[v]) {
          self.controlledViews[v].show();
          self.enabledViews.push(v);
        }
      })
    } else {
      self.enabledViews = [];
      _.each(this.controlledViews, function(v){
        v.show();
        self.enabledViews.push(v);
      })
    }
  },

  _goToLibrary: function() {
    this.routerModel.set({
      shared: 'no',
      library: true,
      page: 1
    });
  },

  _hideBlocks: function(views) {
    var self = this;
    if (views) {
      _.each(views, function(v){
        if (self.controlledViews[v]) {
          self.controlledViews[v].hide();
          self.enabledViews = _.without(self.enabledViews, v);
        }
      })
    } else {
      _.each(this.controlledViews, function(v){
        v.hide();
      });
      self.enabledViews = [];
    }
  },

  _isBlockEnabled: function(name) {
    if (name) {
      return _.contains(this.enabledViews, name);
    }
    return false
  }

});
