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
  _counts: { types: { all: undefined, liked: undefined, locked: undefined, imported: undefined }, categories: {} },

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
    var contentType = this.routerModel.get('content_type');
    var contentTypeKnown = contentType === 'maps' || contentType === 'datasets';
    if (contentTypeKnown) {
      this._fetchCounts(this, function() {
        // refresh view to show new counts if category tree already shown
        if (this.$el.find(".LeftMenuContent").length > 0) {
          this._initCategoryTree(true);
        }
      });
      this._loadCategoryTree();
    }
    this.routerModel.bind('change:content_type', function() {
      this._loadCategoryTree();
      this.updateCounts();
    }, this);
    this.routerModel.bind('change:samples', function() {
      this.updateCounts();
    }, this);
    this.add_related_model(this.routerModel);
  },

  _loadCategoryTree: function() {
    var isMaps = this.routerModel.get('content_type') == "maps";
    var rootId = isMaps ? this._MAPS_CATEGORY : this._DATASETS_CATEGORY;
    this._categoryTree = new cdb.admin.CategoryTree({user: this.options.user, rootId: rootId});
    this._categoryTree.load(this, this._initCategoryTree);
  },

  updateCounts: function() {
    var self = this;
    this._fetchCounts(this, function() {
      this._initCategoryTree(true);
    });
  },

  _fetchCounts: function(callbackContext, onSuccess, onError) {
    var isMaps = this.routerModel.get('content_type') == "maps";
    var self = this;
    $.ajax({
      url: window.location.origin + '/user/' + this.options.user.get('username') + '/api/v1/viz/count',
      data: {
        type: isMaps ? "maps" : "datasets"
      }
    })
    .done(function(response) {
      self._counts = response;

      if (onSuccess) {
        onSuccess.call(callbackContext);
      }
    })
    .fail(function() {
      if (onError) {
        onError.call(callbackContext);
      }
    });
  },

  _initCategoryTree: function(keepCategories) {
    var self = this;
    var isMaps = this.routerModel.get('content_type') == "maps";
    if (!keepCategories) {
      this.collection.reset();
      var rootId = isMaps ? self._MAPS_CATEGORY : self._DATASETS_CATEGORY;
      var categories = this._categoryTree.getChildCategories(rootId);
      var category, id, numCategories = categories.length;
      for (i = 0; i < numCategories; ++i) {
        category = categories[i];
        this.collection.add(category);
      }
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
    });

    this.$el.find(".ParentChevron").click(function(ev){
      var menuItemEl = $(this).closest('.MenuItem');
      var categoryId = menuItemEl.data('category');
      var categoryItem = self.collection.get(categoryId);
      menuItemEl.toggleClass("expanded");
      categoryItem.set('expanded', menuItemEl.hasClass("expanded"));
      $(this).closest('li').find("ul.SubCategories").toggleClass("expanded");
      ev.stopPropagation();
    });
  }

});