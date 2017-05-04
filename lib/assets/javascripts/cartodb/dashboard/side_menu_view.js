var $ = require('jquery-cdb-v3');
var cdb = require('cartodb.js-v3');
cdb.admin = require('cdb.admin');
var Backbone = require('backbone-cdb-v3');
var CategoryTree = require('../table/category_tree');


/**
 *  Categories view.
 *
 *  Select a category of the data library
 *
 */

module.exports = cdb.core.View.extend({

  _categoryTree: undefined,
  _DATASETS_CATEGORY: 1,
  _MAPS_CATEGORY: 2,
  _counts: { all: undefined, liked: undefined, locked: undefined, imported: undefined, samples: undefined },

  initialize: function() {
    this.routerModel = this.options.routerModel;
    this.localStorage = this.options.localStorage;
    // TODO: Get categories using an API.
    this.contentCollection = this.options.collection;
    this.collection = new Backbone.Collection(); 
    this.template = cdb.templates.getTemplate('dashboard/views/side_menu');

    this._initBinds();
  },

  _initBinds: function() {
    this._loadCategoryTree();
    this.routerModel.bind('change:content_type', function() {
      this._loadCategoryTree();
    }, this);
    this.contentCollection.bind('reset', function() {
      var routerParams = this.routerModel.attributes;
      var updateNeeded = this._counts.all === undefined &&
          !(routerParams.category || routerParams.liked || routerParams.imported || routerParams.locked === 'true' || routerParams.q != '' || routerParams.tag != ''); // showing All
      if (updateNeeded) {
        this._updateCounts();
        this._initCategoryTree();
      }
    }, this);
    this.add_related_model(this.routerModel);
  },

  _loadCategoryTree: function() {
    var isMaps = this.routerModel.get('content_type') == "maps";
    var rootId = isMaps ? this._MAPS_CATEGORY : this._DATASETS_CATEGORY;
    this._categoryTree = new cdb.admin.CategoryTree({user: this.options.user, rootId: rootId, counts: true});
    this._categoryTree.load(this, this._initCategoryTree);
  },

  _updateCounts: function() {
    var self = this;
    self._counts = _.defaults({ all: 0, liked: 0, locked: 0, imported: 0, samples: 0 }, self._counts);
    self.contentCollection.each(function(item) {
      if (item.get('liked') == true) {
        self._counts.liked++;
      }
      if (item.get('locked') == true) {
        self._counts.locked++;
      }
      if (item.get('type') == 'table') {
        self._counts.imported++;
      }
      self._counts.all++;
    });
  },

  _initCategoryTree: function() {
    var self = this;
    var isMaps = this.routerModel.get('content_type') == "maps";
    this.collection.reset();
    var rootId = isMaps ? self._MAPS_CATEGORY : self._DATASETS_CATEGORY;
    var categories = this._categoryTree.getChildCategories(rootId);
    var category, id, numCategories = categories.length;
    for (i = 0; i < numCategories; ++i) {
      category = categories[i];
      this.collection.add(category);
    }
    this.$el.html(
      this.template({
        routerModel: this.routerModel,
        content_type: isMaps ? "maps" : "datasets",
        current_category: this.routerModel.get('page'),
        categories: this.collection.models,
        counts: this._counts
      })
    );

    var leftMenuContent = this.$el.find(".LeftMenuContent");

    leftMenuContent.find(".MenuItem").click(function(){
      self.contentCollection.each(function(item) {
        if (item.attributes.selected === true) {
          item.set('selected', false);
        }
      });

      leftMenuContent.find(".MenuItem").removeClass("selected");
      $(this).addClass("selected");
      var contentType = isMaps ? "maps" : "datasets"
      var defaultParams = {
        page: 1,
        category: false,
        imported: false,
        liked: false,
        locked: false,
        samples: false,
        q: '',
        tag: '',
        asc_order: self.routerModel.get('asc_order'),
        order: self.routerModel.get('order')
      };
      var params = _.clone($(this).data("params"));
      var locked = params.locked ? 'true' : ((params.shared || params.liked) ? 'false' : 'always');
      params = _.defaults(params, defaultParams);
      params.locked = locked;

      self.routerModel.set(params);

      /*      
      var order = $(this).data("order");

      if (order) {
        self.localStorage.set({ 'dashboard.order': order });
        self.routerModel.set('order', order, { silent: true });
      }
      */
    });

    this.$el.find(".ParentChevron").click(function(ev){
      $(this).closest('.MenuItem').toggleClass("expanded");
      $(this).closest('li').find("ul.SubCategories").toggleClass("expanded");
      ev.stopPropagation();
    });
  }

});