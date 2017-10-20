/**
 * dropdown when user clicks on a filter sort option
 */
cdb.admin.FilterSortDropdown = cdb.admin.DropdownMenu.extend({

  className: 'dropdown border',

  isPublic: false,
  events: {
    'click .sortAToZ': '_sort_az',
    'click .sortHighToLow': '_sort_high_low',
    'click .default': '_default_sort'
  },

  initialize: function () {
    this.options.width = 120;
    this.elder('initialize');
    this.sortValue = null;
    this.setupBinds();
  },
  initializeSetup: function (reachedLimit) {
    this.reached_limit = reachedLimit;
    if (this.sortValue === null) {
      this.sortValue = (reachedLimit) ? 'Default' : 'HighLowAZ';
    }
  },
  _sort_az: function (e) {
    e.preventDefault();
    this.sortValue = 'AZ';
    this.trigger('sortChange', 'AZ');
    this.hide();
    return false;
  },
  _sort_high_low: function (e) {
    e.preventDefault();
    this.sortValue = 'HighLowAZ';
    this.trigger('sortChange', 'HighLowAZ');
    this.hide();
    return false;
  },
  _default_sort: function (e) {
    e.preventDefault();
    this.sortValue = 'Default';
    this.trigger('sortChange', 'Default');
    this.hide();
    return false;
  },

  setupBinds: function () {
    cdb.god.bind('closeDialogs', function () {
      this.hide();
    }.bind(this));
  },

  render: function () {
    cdb.admin.DropdownMenu.prototype.render.call(this);
    if (this.sortValue === 'HighLowAZ') {
      this.$el.find('.sortAToZ').show();
      this.$el.find('.sortHighToLow').hide();
      this.$el.find('.default').hide();
      if (this.reached_limit) {
        this.$el.find('.default').show();
      }
    } else if (this.sortValue === 'AZ') {
      this.$el.find('.sortAToZ').hide();
      this.$el.find('.sortHighToLow').show();
      this.$el.find('.default').hide();
      if (this.reached_limit) {
        this.$el.find('.default').show();
      }
    } else {
      this.$el.find('.sortAToZ').show();
      this.$el.find('.sortHighToLow').show();
      this.$el.find('.default').hide();
    }
    return this;
  }
});
