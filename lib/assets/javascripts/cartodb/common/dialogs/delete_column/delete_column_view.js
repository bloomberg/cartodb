var cdb = require('cartodb.js-v3');
var BaseDialog = require('../../views/base_dialog/view');
var ViewFactory = require('../../view_factory');
var randomQuote = require('../../view_helpers/random_quote');

module.exports = BaseDialog.extend({

  initialize: function() {
    this.elder('initialize');

    if (!this.options.table) {
      throw new Error('table is required');
    }

    if (!this.options.column) {
      throw new Error('column is required');
    }
    this._initViews();
    this._initBinds();
  },

  render_content: function() {
    return this._panes.getActivePane().render().el;
  },

  _initViews: function() {
    var column_alias = null;
    if (this.options.table.get('column_aliases') && this.options.table.get('column_aliases')[this.options.column]) {
      column_alias = this.options.table.get('column_aliases')[this.options.column];
    }

    this.table = this.options.table;
    this.column = column_alias || this.options.column;

    this._panes = new cdb.ui.common.TabPane({
      el: this.el
    });
    this.addView(this._panes);
    this._panes.addTab('confirm',
      ViewFactory.createByTemplate('common/dialogs/delete_column/template', {
        column: this.column
      })
    );
    this._panes.addTab('loading',
      ViewFactory.createByTemplate('common/templates/loading', {
        title: 'Deleting column…',
        quote: randomQuote()
      })
    );
    this._panes.addTab('fail',
      ViewFactory.createByTemplate('common/templates/fail', {
        msg: 'Could not delete column for some reason'
      })
    );
    this._panes.active('confirm');
  },

  _initBinds: function() {
    this._panes.bind('tabEnabled', this.render, this);
  },

  ok: function() {
    this._panes.active('loading');
    this.table.deleteColumn(this.options.column);
    this.close();
  }
});
