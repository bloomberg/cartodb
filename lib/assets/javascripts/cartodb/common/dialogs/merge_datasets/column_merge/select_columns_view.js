var $ = require('jquery-cdb-v3');
var cdb = require('cartodb.js-v3');
var ColumnsSelectorView = require('../columns_selector_view');
var StickyHeaderView = require('../sticky_header_view');
var FooterView = require('../footer_view');
var FooterInfoView = require('./footer_info_view');

/**
 * View to select the columns to merge.
 */
module.exports = cdb.core.View.extend({

  initialize: function() {
    this._initViews();
    this._initBinds();
  },

  render: function() {
    var leftKeyColumn = this.model.get('leftKeyColumn');
    var leftKeyColumnName = leftKeyColumn && leftKeyColumn.get('name');
    var leftKeyColumnAlias = leftKeyColumn && leftKeyColumn.get('alias');
    var leftKeyColumnType = leftKeyColumn && leftKeyColumn.get('type');

    var rightKeyColumn = this.model.get('rightKeyColumn');
    var rightKeyColumnName = rightKeyColumn && rightKeyColumn.get('name');
    var rightKeyColumnAlias = rightKeyColumn && rightKeyColumn.get('alias');
    var rightKeyColumnType = rightKeyColumn && rightKeyColumn.get('type');

    var $el = $(
      this.getTemplate('common/dialogs/merge_datasets/column_merge/select_columns')({
        leftKeyColumnName: leftKeyColumnName,
        leftKeyColumnAlias: leftKeyColumnAlias,
        leftKeyColumnType: leftKeyColumnType,
        rightKeyColumnName: rightKeyColumnName,
        rightKeyColumnAlias: rightKeyColumnAlias,
        rightKeyColumnType: rightKeyColumnType,
        aliasUser: this.options.user.featureEnabled('aliases')
      })
    );
    $el.find('.js-sticky-header').append(this._stickyHeaderView.render().$el);
    $el.find('.js-left-table').append(this._leftTableComboView.render().$el);
    $el.find('.js-left-columns').append(this._leftColumnsView.render().$el);
    $el.find('.js-right-table').append(this._rightTableComboView.render().$el);
    $el.find('.js-right-columns').append(this._rightColumnsView.render().$el);
    $el.append(this._footerView.render().$el);
    this.$el.html($el);

    return this;
  },

  onChangeKeyColumnsVisiblity: function() {
    this._stickyHeaderView.$el.slideToggle(200);
  },

  _initViews: function() {
    this._stickyHeaderView = new StickyHeaderView({
      leftKeyColumn: this.model.get('leftKeyColumn'),
      rightKeyColumn: this.model.get('rightKeyColumn')
    });
    this.addView(this._stickyHeaderView);

    var leftTable = this.model.get('leftTable');
    var leftTableName = presentAlias({
      user: this.options.user,
      original: leftTable.get('name'),
      alias: leftTable.get('name_alias')
    });

    this._leftTableComboView = new cdb.forms.Combo({
      className: 'Select',
      width: '100%',
      disabled: true,
      extra: [leftTableName]
    });
    this.addView(this._leftTableComboView);

    this._leftColumnsView = new ColumnsSelectorView({
      collection: this.model.get('leftColumns'),
      selectorType: 'switch',
      user: this.options.user
    });
    this.addView(this._leftColumnsView);

    var rightTable = this.model.get('rightTable');
    var rightTableName = presentAlias({
      user: this.options.user,
      original: rightTable.get('name'),
      alias: rightTable.get('name_alias'),
    });

    this._rightTableComboView = new cdb.forms.Combo({
      className: 'Select',
      width: '100%',
      disabled: true,
      extra: [rightTableName]
    });
    this.addView(this._rightTableComboView);

    this._rightColumnsView = new ColumnsSelectorView({
      collection: this.model.get('rightColumns'),
      selectorType: 'switch',
      user: this.options.user
    });
    this.addView(this._rightColumnsView);

    this._footerView = new FooterView({
      model: this.model,
      nextLabel: 'Merge datasets',
      infoView: new FooterInfoView({
        model: this.model
      })
    });
    this.addView(this._footerView);
  },

  _initBinds: function() {
    this.model.get('leftColumns').bind('change:selected', this._onChangeSelectedColumn, this);
    this.add_related_model(this.model.get('leftColumns'));

    this.model.get('rightColumns').bind('change:selected', this._onChangeSelectedColumn, this);
    this.add_related_model(this.model.get('rightColumns'));
  },

  _onChangeSelectedColumn: function(column, isSelected) {
    this.model.onlyAllowOneSelectedTheGeomColumn(column, isSelected);
  }

});
