var cdb = require('cartodb.js-v3');

/**
 *  Editable description
 */
module.exports = cdb.core.View.extend({

  events: {
    "click .js-add-btn": "_edit",
    "click .js-field-input": "killEvent",
    "blur .js-field-input": "_cancelEditing",
    "keydown .js-field-input": "_keyPressed"
  },

  options: {
    editable: true,
    maxLength: 200
  },

  initialize: function() {
    this.template = cdb.templates.getTemplate('dashboard/editable_fields/editable_text');
    this.field = this.options.field || 'description';

    // Backbone's className won't work here because we are providing an el
    this.$el.addClass('EditableField');
  },

  render: function() {
    var safeHTML = cdb.core.sanitize.html(markdown.toHTML(this.model.get(this.field) || ''))
    var value = {
      safeHTML: safeHTML,
      clean: cdb.Utils.stripHTML(safeHTML)
    };
    this.$el.html(this.template({
      editText: this.options.editText || 'Add Description...',
      undefinedText: this.options.undefinedText || 'No description',
      value: value,
      isEditing: this.$el.hasClass('is-editing'),
      editable: this.options.editable,
      maxLength: this.options.maxLength
    }));

    return this;
  },

  _edit: function(ev) {
    this.killEvent(ev);

    if (this.options.editable) {
      this.$el.addClass('is-editing');
      this.render();
      this.$('.js-field-input').select().focus();
    }
  },

  _keyPressed: function(ev) {
    var escPressed = (ev.keyCode == $.ui.keyCode.ESCAPE);
    var cmdEnterPressed = ((ev.metaKey || ev.ctrlKey) &&  ev.keyCode == $.ui.keyCode.ENTER);
    var enterPressed = (ev.keyCode == $.ui.keyCode.ENTER);
    var currentText = this.$('.js-field-input').val();

    if (cmdEnterPressed) {
      ev.preventDefault();
      this._addNewLine();
    } else if (enterPressed && currentText.trim() != '') {
      ev.preventDefault();
      this._save();
    } else if (escPressed) {
      ev.stopPropagation();
      this._cancelEditing();
    }
  },

  _addNewLine: function() {
    var $input = this.$('.js-field-input');
    $input.val($input.val() + "\n");

    // Scroll to bottom of the textarea
    $input[0].scrollTop = $input[0].scrollHeight;
  },

  _save: function() {
    var attributes = {};
    attributes[this.field] = this.$('.js-field-input').val();
    this.model.save(attributes);
    this.$el.removeClass('is-editing');
    this.render();
  },

  _cancelEditing: function() {
    this.$el.removeClass('is-editing');
    this.render();
  }
});
