var cdb = require('cartodb.js-v3');
var _ = require('underscore-cdb-v3');
var DatasetsItem = require('./datasets/datasets_item');
var MapsItem = require('./maps/maps_item');
var SampleMapItem = require('./maps/sample_map_item');
var DeepInsightsItem = require('./maps/deep_insights_item');
var PlaceholderItem = require('./maps/placeholder_item_view');
var PlaceholderItemFirstMap = require('./maps/placeholder_item_first_map_view');
var RemoteDatasetsItem = require('./datasets/remote_datasets_item');
var MapTemplates = require('../common/map_templates');
var MAP_CARDS_PER_ROW = 3;

/**
 *  View representing the list of items
 */

module.exports = cdb.core.View.extend({
  tagName: 'div',
  className: 'ContentListContainer',

  events: {},

  _ITEMS: {
    'remotes': DatasetsItem,
    'datasets': DatasetsItem,
    'deepInsights': DeepInsightsItem,
    'maps': MapsItem,
    'samples':SampleMapItem
  },

  _container : undefined,
  _stateContainer: undefined,
  _originalItemsDirty: true,
  _allItems: [],
  _fuse: undefined,
  _searchInputEl: undefined,
  _headerPadded: true,
  _MAX_SEARCH_RESULTS: 30,
  _MAX_INSTANT_SEARCH_ITEMS: 300,
  _searchTimer: undefined,

  initialize: function() {
    var self = this;
    this.router = this.options.router;
    this.routerModel = this.options.routerModel;
    this.createModel = this.options.createModel;
    this.user = this.options.user;
    this.parentEl = this.options.parentEl;
    this._initBinds();
    this._originalItemsDirty = true;

    this.collection.bind('reset', function() {
      if (this._originalItemsDirty) {
        this._allItems = this.collection.models;
        this._updateSearchIndex();
        this._originalItemsDirty = false;
      }
    }, this);
  },

  _initializeSearch: function() {
    if (this._searchInputEl !== undefined) {
      return;
    }

    var self = this;
    this._searchInputEl = 
      this.$el.closest('.ContentController').find('.search-bar-input')
      .on('keydown', function(e) {
        var keyCode = e.keyCode;

        if (keyCode == 27 ) {
           e.stopPropagation();
           e.preventDefault();
        }
      })
      .on('keyup search', function(e) {
        var keyCode = e.keyCode;
        var resetSearch = false, skipSearch = false;
        var q = self._searchInputEl.val().toLowerCase();
        if (keyCode == 27) {
          resetSearch = true;
        } else if (q == self.routerModel.get('q')) {
          skipSearch = true;
        }

        if (!skipSearch && q == '') {
          resetSearch = true;
        }

        if (resetSearch) {
          self.routerModel.set('q', '', { silent: true });
          self.collection.reset(self._allItems);
          self._searchInputEl.val('');
        } else if (!skipSearch) {
          if (self._allItems.length <= self._MAX_INSTANT_SEARCH_ITEMS) {
            self._doSearch();
          } else {
            self._requestSearch();
          }
        }
        e.stopPropagation();
      });

    this.routerModel.bind('change', function() {
      this._originalItemsDirty = true;
      this._searchInputEl.val('');
    }, this);
  },

  _requestSearch: function() {
    var self = this;

    clearTimeout(self._searchTimer); // prevent unnecessary requests

    self._searchTimer = setTimeout(function() {
      self._doSearch();
    }, 500);
  },

  _doSearch: function() {
    var self = this;
    var q = self._searchInputEl.val().toLowerCase();

    var fuseResults = self._fuse.search(q);
    var filtered = [];
    _.each(fuseResults, function(item, index) {
      if (filtered.length >= self._MAX_SEARCH_RESULTS) {
        return;
      }
      filtered.push(self._allItems[fuseResults[index].id]);
    });

    self.routerModel.set('q', q, { silent: true });
    self.collection.reset(filtered);
  },

  _onTableHeaderClicked: function(e) {
    var self = e.data;
    var prevOrderBy = self.routerModel.get('order') || 'name';
    var prevAscOrder = self.routerModel.get('asc_order') !== false;

    e.stopPropagation();

    if (prevOrderBy == e.currentTarget.id) {
      self.routerModel.set('asc_order', !prevAscOrder);
    } else {
      self.routerModel.set('asc_order', e.currentTarget.id !== 'likes', { silent: true });
      self.routerModel.set('order', e.currentTarget.id);
    }
  },

  _updateHeaderPadding: function() {
    if (!this.routerModel.isDatasets()) {
      return;
    }
    var tableContainer = this.$el.find('.DatasetsTableScrollable');
    var scrollHeight = tableContainer.prop('scrollHeight');
    var containerHeight = tableContainer.height();
    if (scrollHeight == 0 || containerHeight == 0) {
      return;
    }
    var hasScrollBar = scrollHeight > containerHeight;
    var headerContainer = this.$el.find('.DatasetsTableHeaderContainer');
    if (hasScrollBar) {
      headerContainer.addClass('padded');
    } else {
      headerContainer.removeClass('padded');
    }
    this._headerPadded = hasScrollBar;
  },

  render: function($parent) {
    var self = this;
    this.clearSubViews();
    this.$el.empty();

    if (this.routerModel.isDatasets()) {
      var prevOrderBy = self.routerModel.get('order') || 'name';
      var prevAscOrder = self.routerModel.get('asc_order') !== false;
      var orderIcon = '<div class="' + (prevAscOrder ? 'DatasetsTableHeaderOrderAsc' : 'DatasetsTableHeaderOrderDesc') + '"></div>';

      this.$el.find('.DatasetsTableHeader .DatasetsTableCell.sortable').off('click', self._onTableHeaderClicked);

      this.$el.html(
        '<div class="DatasetsTableHeaderContainer' + (this._headerPadded ? ' padded' : '') + '">\
          <div class="DatasetsTable">\
            <div class="DatasetsTableHeader">\
              <div class="DatasetsTableRow">\
                <div class="DatasetsTableCell"></div>\
                <div class="DatasetsTableCell sortable" id="name">Name</div>\
                <div class="DatasetsTableCell sortable" id="description">Description</div>\
                <div class="DatasetsTableCell sortable" id="source">Source</div>\
                <div class="DatasetsTableCell sortable" id="likes"></div>\
              </div>\
            </div>\
          </div>\
        </div>\
        <div class="DatasetsTable DatasetsTableScrollable">\
          <div class="DatasetsTable"><div class="TableBody"></div></div>\
        </div>');

      this.$el.find('.DatasetsTableHeader .DatasetsTableCell#' + prevOrderBy).append(orderIcon);
      this.$el.find('.DatasetsTableHeader .DatasetsTableCell.sortable').on('click', self, self._onTableHeaderClicked);
      this._container = this.$el.find('.TableBody');
    } else {
      this.$el.html('<div class="MapsListScrollable"><ul class="MapsList"></ul></div>');
      this._container = this.$el.find('.MapsList');
    }
    this._stateContainer = this.parentEl;

    this.collection.each(this._addItem, this);

    if (this.collection.total_entries == 0 && this.routerModel.isMaps()) {
      this._showFirstMapPlaceholderItem();
    }

    setTimeout(function() {
      self._updateHeaderPadding();
    }, 20);

    setTimeout(function() {
      self._initializeSearch();
    }, 1000);

    return this;
  },

  show: function() {
    this._stateContainer.removeClass('is-hidden');
  },

  hide: function() {
    this._stateContainer.addClass('is-hidden');
  },

  _addItem: function(m) {
    var type = this.routerModel.get('content_type');

    if (m.get('type') === "remote" && this.routerModel.isDatasets()) {
      type = "remotes";
    }

    if (this.routerModel.isDeepInsights()) {
      type = "deepInsights";
    }

    if (this.routerModel.get('samples') === true) {
      type = "samples";
    }

    var item = new this._ITEMS[type]({
      model:       m,
      router:      this.router,
      routerModel: this.routerModel,
      createModel: this.createModel,
      user:        this.user,
      type:        m.get('type')
    });

    this.addView(item);
    this._container.append(item.render().el);
  },

  _initBinds: function() {
    var self = this;
    this.collection.bind('loading', this._onItemsLoading, this);
    this.collection.bind('reset', this.render, this);
    this.add_related_model(this.collection);

    $(window).resize(function() {
      self._updateHeaderPadding();
    });
  },

  _onItemsLoading: function() {
    this._stateContainer.addClass('is-loading');
  },

  _fillEmptySlotsWithPlaceholderItems: function() {
    var mapTemplates = _.shuffle(MapTemplates);
    _.times(this._emptySlotsCount(), function(i) {
      var d = mapTemplates[i];
      if (d) {
        var m = new cdb.core.Model(d);
        var view = new PlaceholderItem({
          model: m,
          collection: this.collection
        });
        this._container.append(view.render().el);
        this.addView(view);
      }
    }, this);
  },

  _showFirstMapPlaceholderItem: function() {
    var view = new PlaceholderItemFirstMap({
      model: {},
      collection: this.collection
    });
    this._container.append(view.render().el);
    this.addView(view);
  },

  _emptySlotsCount: function() {
    return (this.collection._ITEMS_PER_PAGE - this.collection.size()) % MAP_CARDS_PER_ROW;
  },

  _updateSearchIndex: function() {
    var searchItems = [];
    var id = 0;
    this.collection.each(function (item) {
      var name = item.get('table').name_alias || item.get('display_name') || item.get('name');
      searchItems.push({
        id: id++,
        name: name
      });
    }, this);

    this._fuse = new Fuse(searchItems, { keys: ["name"], threshold: 0.4 });
  }

});
