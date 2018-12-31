var cdb = require('cartodb.js-v3');
var moment = require('moment');
var Utils = require('cdb.Utils');
var navigateThroughRouter = require('../../common/view_helpers/navigate_through_router');
var pluralizeString = require('../../common/view_helpers/pluralize_string');
var LikesView = require('../../common/views/likes/view');
var EditableText = require('../../dashboard/editable_fields/editable_text');
var EditableTags = require('../../dashboard/editable_fields/editable_tags');
var SyncView = require('../../common/dialogs/sync_dataset/sync_dataset_view');

/**
 * View representing an item in the list under datasets route.
 */
module.exports = cdb.core.View.extend({

  tagName: 'div',
  className: 'DatasetsTableRow DatasetsTableRow--selectable',

  events: {
    'click .js-tag-link': navigateThroughRouter,
    'click .js-privacy': '_openPrivacyDialog',
    'click .js-sync': '_openSyncDialog',
    'click': '_selectDataset'
  },

  initialize: function() {
    this.user = this.options.user;
    this.routerModel = this.options.routerModel;
    this.template = cdb.templates.getTemplate('dashboard/views/datasets_table_item');

    this._initBinds();
  },

  render: function() {
    this.clearSubViews();
    var vis = this.model;
    var table = vis.tableMetadata();
    var isOwner = this.model.permission.isOwner(this.user);
    var isCommonData = vis.get('type') == 'remote' || vis.get('from_external_source') == true;

    var url = vis.viewUrl(this.user);
    url = (this.routerModel.get('liked') && !vis.permission.hasAccess(this.user)) ? url.public() : url.edit();
    var likes = vis.get('likes') || 0;

    var d = {
      title:                   this._title(
                                 vis.get('name'),
                                 vis.get('display_name') || vis.get('name_alias'),
                                 this.user
                               ),
      datasetUrl:              encodeURI(url),
      isOwner:                 isOwner,
      owner:                   vis.permission.owner.renderData(this.user),
      privacy:                 vis.get('privacy').toLowerCase(),
      likes:                   likes,
      timeDiff:                moment(vis.get('updated_at')).fromNow(),
      routerModel:             this.routerModel,
      maxTagsToShow:           3,
      syncStatus:              undefined,
      syncRanAt:               undefined,
      fromExternalSource:      "",
      type:                    this.options.type,
      source:                  vis.get('source')
    };

    this.$el.html(this.template(d));

    var descView = new EditableText({
      el: this.$('.js-item-description'),
      model: this.model,
      editable: isOwner && !isCommonData
    });
    this.addView(descView.render());

    var sourceView = new EditableText({
      el: this.$('.js-item-source'),
      model: this.model,
      field: 'source',
      editText: 'Add Source...',
      undefinedText: 'No source',
      editable: isOwner && !isCommonData
    });
    this.addView(sourceView.render());
    
    var likesView = new LikesView({
      model: new cdb.admin.Like.newByVisData({
          vis_id: vis.get('id'),
          liked: likes > 0,
          likes: likes,
          likeable: !vis.get('needs_cd_import')
        })
    });
    likesView.model.bind('toggled', function() {
      this.trigger('vis-states-changed');
    }, this);
    this.$('.js-likes-indicator').replaceWith(likesView.render().el);
    this.addView(likesView);

    // Item selected?
    var selected = vis.get('selected');
    this.$el[ selected ? 'addClass' : 'removeClass' ]('is--selected');
    this.$el.find('.RowCheckbox')[ selected ? 'addClass' : 'removeClass' ]('is--selected');

    return this;
  },

  _title: function (title, alias, user) {
    if (alias && user.featureEnabled('aliases') && title != alias) {
      return alias + ' (' + title + ')';
    } else {
      return alias || title;
    }
  },

  _initBinds: function() {
    this.model.on('change', this.render, this);
  },

  _openPrivacyDialog: function(ev) {
    this.killEvent(ev);
    cdb.god.trigger('openPrivacyDialog', this.model);
  },

  _openSyncDialog: function(ev) {
    this.killEvent(ev);
    var view = new SyncView({
      clean_on_hide: true,
      enter_to_confirm: true,
      table: this.model.tableMetadata()
    });

    // Force render of this item after changing sync settings
    var self = this;
    var originalOK = view.ok;
    view.ok = function() {
      originalOK.apply(view, arguments);
      self.model.fetch(); // to force a re-render due to possible changed sync settings
    };

    view.appendToBody();
  },

  _selectDataset: function(ev) {
    // Let links use default behaviour
    if (ev.target.tagName !== 'A') {
      this.killEvent(ev);

      if (this.options.createModel !== undefined) {
        if (!this.model.get('selected')) {
          this.options.createModel.clearSelection();
        }
      }

      this.model.set('selected', !this.model.get('selected'));
    }
  }

});
