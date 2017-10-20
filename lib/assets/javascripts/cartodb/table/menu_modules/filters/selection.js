(function() {

  // Selection Filter
  var SelectorView = cdb.core.View.extend({
    tagName: 'li',
    events: {
      'click': 'toggle',
      'click .remove-item': '_removeItem'
    },

    initialize: function() {
      this.model.bind('change', this.render, this);
      if (!this.options.reached_limit) {
        this.template_base = _.template("<p class='<% if (bucket == undefined || bucket == '' || bucket == 'null') { %>empty<% } %>'><% if (bucket == undefined) { %>null<% } else if (bucket == '') {%>empty<% } else { %><%- bucket %><% } %></p> <div class='value'><%- value %></div>");
      }else{
        this.template_base = _.template(
          "<p class='"+
            "<% if (bucket == undefined || bucket == '' || bucket == 'null') {" +
              "%>empty<%" +
            "} %>'>" +
            "<% if (bucket == undefined) {"+
              "%>null<%" +
            "} else if (bucket == '') {" +
              "%>empty<%" +
            "} else { %>" +
              "<%- bucket %><%" +
            "} %>"+
          "</p>" +
          "<div class='value-largeset'>" +
            "<%- value %>" +
          "</div>" +
          "<p class='remove-item'></p>");
      }
    },

    /*
     * Adds thousands separators.
     **/
    _formatNumber:function(x) {
      var parts = x.toString().split(".");
      parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      return parts.join(".");
    },

    _removeItem: function(e){
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      this.trigger('removeSelector', this.model);
    },

    _cleanString: function(s) {

      var n = 180; // truncate length

      if (s) {
        s = s.replace(/<(?:.|\n)*?>/gm, ''); // strip HTML tags
        s = s.substr(0, n-1) + (s.length > n ? '&hellip;' : ''); // truncate string
      }

      return s;

    },

    render: function() {

      var pretty_bucket;
      var bucket_name = this.model.get("bucket");

      if (this.options.column_type == 'boolean') {

        if (bucket_name == null) pretty_bucket = "null";
        else pretty_bucket = (bucket_name) ? "true" : "false";

      } else {
        pretty_bucket = this._cleanString(bucket_name);
      }

      // Format the number
      var value = this.model.get("value");
      this.model.set("value", this._formatNumber(value));

      this.$el.html(this.template_base(_.extend(this.model.toJSON(), this.options, { bucket: pretty_bucket } )));

      if(this.model.get('selected')) {
        this.$el.addClass('selected');
      } else {
        this.$el.removeClass('selected');
      }

      return this;

    },

    toggle: function(e) {
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      var m = this.model.get('selected');
      this.model.set('selected', !m);
      this.trigger("updateCounter", this.el);
      this.trigger("updateInputFocus");
      if(cdb.god.currentFilterLi){
        cdb.god.currentFilterLi.removeClass('li-focus-item');
      }
      cdb.god.currentFilterLi = $(this.el);
      cdb.god.currentFilterLi.addClass('li-focus-item');
    }
  });

  // Categorical Filter

  var OperationView = cdb.core.View.extend({

    tagName: 'li',

    events: {
      'click a.remove-inline': '_remove',
      'change select.operation': '_onOperationChanged',
      'change select.operator': '_onOperatorChanged',
      'change input[type="text"]': '_onTextChanged'
    },

    OPERATORS: {
      'OR': 'Or',
      'AND': 'And',
    },
    OPERATIONS: {
      'BEGINSWITH': 'Begins with',
      'CONTAINS': 'Contains',
      'ENDSWITH': 'Ends with',
      'EQUALS': 'Equals'
    },

    initialize: function() {
      this.model.bind('change', this.render, this);
      this.template = this.getTemplate('table/menu_modules/filters/templates/operation');
    },

    _cleanString: function(s) {

      var n = 180; // truncate length

      if (s) {
        s = s.replace(/<(?:.|\n)*?>/gm, ''); // strip HTML tags
        s = s.substr(0, n-1) + (s.length > n ? '&hellip;' : ''); // truncate string
      }

      return s;

    },

    _onTextChanged: function (event) {
      var value = $(event.target).val();
      this.model.set('text', value);
    },

    _onOperatorChanged: function (event) {
      var value = $(event.target).val();
      this.model.set('operator', value);
    },

    _onOperationChanged: function (event) {
      var value = $(event.target).val();
      this.model.set('operation', value);
    },

    render: function() {

      this.$el.html(this.template(_.extend(this.model.toJSON(), {
        first: this.model.collection.indexOf(this.model) === 0,
        operators: this.OPERATORS,
        operations: this.OPERATIONS
      } )));

      return this;
    },

    _remove: function(e) {
      e.preventDefault();
      e.stopPropagation();
      if(cdb.god.currentFilterLi) {
        cdb.god.currentFilterLi.removeClass('li-focus-item');
        cdb.god.currentFilterLi = null;
      }
      this.model.destroy();
    }

  });

  cdb.admin.mod.SelectorFilter = cdb.core.View.extend({

    tagName: 'li',
    className: 'filter selection',

    _SCROLLSTEP: 120,

    events: {
      'keypress input[name="query"]': '_onKeyPress',
      'click a.apply':            '_onApply',
      'click a.remove':           '_remove',
      'click .reset':             '_selectNone',
      'click .x':                 '_clearFilterInput',
      'click a.up':               '_move',
      'click a.down':             '_move',
      'click a.all':              '_select',
      'click a.none':             '_select',
      'click .view_mode label':   '_toggleSwitch',
      'click a.toggleButton':     '_toggleFilterView',
      'click a.sort-high-low':    '_showSortDropdown',
      'click a.sort-a-z':         '_showSortDropdown',
      'click a.sort-default':     '_showSortDropdown',
      'click .select_all':        '_selectAll'
    },

    initialize: function() {
      _.bindAll(this, "_updateCounter");
      this.fieldsHidden = false;
      this.selectedCount = 0;
      this.sortFilterSelection = null;
      this.userSelectedSort = false;
      this.listFilterValue = null;
      this.listFilterValueRemote = null;
      this.focusInputField = false;
      this.defaultItems = this._copyDefaultItemsOrder();
      this.toggle = this.model.get('toggle');
      
      this.sortFilterDropdown = new cdb.admin.FilterSortDropdown({
        position: 'position',
        horizontal_position: 'right',
        tick: 'left',
        template_base: 'table/menu_modules/filters/templates/sort_dropdown',
        model: this.model
      });
      this.sortFilterDropdown.bind('sortChange', this._updateSortSetting, this);
      this.model.items.bind('reset',        this.render, this);
      this.model.operations.bind('add',     this._renderOperations, this);
      this.model.operations.bind('remove',  this._renderOperations, this);
      this.model.operations.bind('reset',  this._renderOperations, this);
      this.model.bind('change:controllers', this._toggleControllers, this);
      this.model.bind('change:legend',      this._updateLegend,      this);
      this.model.bind('change:show_items',   this._toggleItems,   this);
      cdb.god.on('closeDialogs', this._closeDialogs, this);
      this.add_related_model(this.model.items);
      this.add_related_model(this.model.operations);

    },

    _toggleFilterView: function(e) {
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      this.toggle = !this.toggle;
      this.model.set('toggle', this.toggle);
      $(this.$el.find('.filterListandControls')).toggle();
    },

    _cleanString: function(s) {

      if (s) {
        s = s.replace(/<(?:.|\n)*?>/gm, ''); // strip HTML tags
      }

      return s;

    },
    _closeDialogs: function () {
      if(cdb.god.currentFilterLi) {
        cdb.god.currentFilterLi.removeClass('li-focus-item');
        cdb.god.currentFilterLi = null;
      }
    },

    _copyDefaultItemsOrder: function() {
      return this.model.items.map(function(r){
        return {bucket: r.attributes.bucket}
      }.bind(this));
    },

    _addUserSelectedFilterField: function(filterField) {
      for(var i = 0; i < this.model.items.models.length; i++){
        //check to see if the selected item was alredy added return if yes otherwise continue
        if(this.model.items.models[i].get('bucket') === filterField.label){
          return
        }
      }
      this.model.items.add({bucket: filterField.label, selected: true, value: filterField.value});
      this.defaultItems.push({bucket: filterField.label})
      this.render();
    },

    _setupAutoComplete: function () {
      if(this.$el.find('.filterInputRemote').length > 0){
        this.$el.find('.filterInputRemote').autocomplete({
          source: function(request, response) {
            var column = this.model.get('column');
            var sql = "SELECT " + column + " as label, count(*) as value From " + this.model.table.id +
              " WHERE " + column + " ILIKE '" + request.term + "%' GROUP BY " + column;
            $.ajax( {
              type: "GET",
              data: "q=" + encodeURIComponent(sql) + "&api_key=" + cdb.config.get('api_key'),
              url: cdb.config.prefixUrl() + '/api/v2/sql',
              success: function( data ) {
                response( data.rows );
              }
            } );
          }.bind(this),
          minLength: 2,
          select: function(e, ui){
            e.preventDefault();
            e.stopPropagation();
            this._addUserSelectedFilterField(ui.item);
          }.bind(this)
        })
        .data("ui-autocomplete")._renderItem = function( ul, item ) {
          var a = null;
          return $( "<li>" )
            .data("item.autocomplete", item)
            .append( "<a>" + item.label + " <span style=\"float:right\">" + item.value  + "</span></a>" )
            .appendTo( ul );
        }
      }
    },

    _clearFilterInput: function () {
      this.listFilterValue = null;
      this.focusInputField = false;
      this.render();
    },

    _updateSortSetting: function (setting) {
      this.sortFilterSelection = setting;
      this.userSelectedSort = true;
      this.render();
    },

    _updateLegend: function () {
      this.$(".legend").html(this.model.get("legend"));
    },

    _updateCounter: function () {
      var count = _.countBy(this.model.items.models, function (m) { return m.get("selected") ? "selected" : "unselected"; });
      this.model.set("count", count);

      if (count.selected == _.size(this.model.items)) {
        this.model.set({ all: true });
        this.$all.addClass("selected");
        this.$none.removeClass("selected");
      }
      else if (!count.selected) {
        this.$none.addClass("selected");
        this.$all.removeClass("selected");
      } else {
        this.$none.addClass("selected");
        this.$all.removeClass("selected");
      }

      var c = count.selected == undefined ? 0 : count.selected;
      this.$el.find(".count").html(" - " + c + " selected");
      this.totalCount = (this.totalCount < 4999) ? this.totalCount : '5000+';
      this.$filter_counter.html((count.selected > 0) ? "(" + count.selected + " of " + this.totalCount + ")" : "(" + this.totalCount + ")");
      if(count.selected > 0){
        this.$filterReset.show();
        this.$selectAll.hide();
      }else{
        this.$filterReset.hide();
        this.$selectAll.show();
      }
      if (this.model.items.length === 0) {
        this.$selectAll.hide();
      }
      //if filter input is empty take focus away
    },		  
    _showSortDropdown: function(e) {
      //close all other dialogs instances before opening this instance
      cdb.god.trigger('closeDialogs');
      this.killEvent(e);
      var container = $(e.target).parent().parent().parent();
      container.append(this.sortFilterDropdown.el);
      this.sortFilterDropdown.openAt(200, 75);
      this.sortFilterDropdown.initializeSetup(this.model.get('reached_limit'));
      this.sortFilterDropdown.render();
      this.sortFilterDropdown.show();

    },

    _showControllers: function() {
      this.model.set("controllers", true)
    },

    _hideControllers: function() {
      this.model.set("controllers", false)
    },

    _toggleControllers: function() {

      if (this.model.get("controllers")) this.$el.find(".controllers").fadeIn(250);
      else this.$el.find(".controllers").fadeOut(250);

    },

    _showScrollers: function() {
      this.model.set("scrollers", true)
      this.$el.find(".fields").addClass("has_scrollers");
    },

    _hideScrollers: function() {
      this.model.set("scrollers", false)
      this.$el.find(".fields").removeClass("has_scrollers");
    },

    _hideItems: function() {
      this.model.set("show_items", false)
    },

    _toggleItems: function() {

      if (this.model.get("show_items")) this.$el.find(".items").fadeIn(250);
      else this.$el.find(".items").fadeOut(250);

    },

    _addItems: function() {
      if (!this.model.get('reached_limit')) {
        this.totalCount = (this.model.items) ? this.model.items.length : 0;
      }else{
        this.totalCount = (this.model.get('total_count')) ? this.model.get('total_count') : 0;
      }
      var self = this;
      var filterList = null; 
      if(this.sortFilterSelection === 'HighLowAZ'){
        filterList = _.clone(this.model.items);
        this.sortArrayListHighLowAZ(filterList);
      }else if(this.sortFilterSelection === 'AZ') {
        filterList = _.clone(this.model.items);
        this.sortArrayListAZ(filterList);
      }else if(this.model.get('reached_limit')){
        filterList = _.clone(this.model.items);
        this._restoreDefaultOrder(filterList);
      }else{
        filterList = _.clone(this.model.items);
      }

      filterList.each(function(m) {
        if(self.listFilterValue){
          if(typeof(m.get('bucket')) === 'string' && m.get('bucket').toLowerCase().indexOf(self.listFilterValue.toLowerCase()) > -1){
              var v = new SelectorView({ model: m, column_type: self.model.get("column_type"), reached_limit:self.model.get('reached_limit') });
              v.bind("updateCounter", self._updateCounter, self);
              v.bind("updateInputFocus", self._updateInputFocus, self);
              v.bind('removeItem', self._removeSelectorView,  self);
              self.$items.append(v.render().el);
              self.addView(v);
          }
        }else{
          var v = new SelectorView({ model: m, column_type: self.model.get("column_type"), reached_limit:self.model.get('reached_limit') });
          v.bind("updateCounter", self._updateCounter, self);
          v.bind("updateInputFocus", self._updateInputFocus, self);
          v.bind('removeSelector', self._removeSelectorView,  self);
          self.$items.append(v.render().el);
          self.addView(v);
        }
      });

    },

    sortArrayListHighLowAZ: function (items) {
      items.models.sort(function (a, b) {
        var x = parseFloat(a.get('value'));
        var y = parseFloat(b.get('value'));
        if (x === y) {
          var name1 = (a.get('bucket')) ? a.get('bucket').toLowerCase() : "";
          var name2 = (b.get('bucket')) ? b.get('bucket').toLowerCase() : "";
          if(name1 === name2){
            return 0
          }
          return name1 < name2 ? -1 : name1 > name2 ? 1 : 0
        }
        if(x !== y){
          return x < y ? 1 : x > y ? -1 : 0
        }
      });
      return items;
    },

    _restoreDefaultOrder: function (items) {
      items.models.sort(function (a, b) {
        for(var k = 0; k < this.defaultItems.length; k++){
          if(this.defaultItems[k].bucket === a.get('bucket')) {
            var orignalPositionA = {bucket: this.defaultItems[k], index: k};
          }
          if(this.defaultItems[k].bucket === b.get('bucket')) {
            var orignalPositionB = {bucket: this.defaultItems[k], index: k};
          }
        }
        return (orignalPositionA.index < orignalPositionB.index) ? -1 : 1;  
      }.bind(this));
      return items;
    },

    _removeSelectorView: function(SelectorView) {
      this.model.items.remove(SelectorView);
      var index = -1;
      for(var k = 0; k < this.defaultItems.length; k++){
        if(SelectorView.get('bucket') === this.defaultItems[k].bucket){
          index = k;
        }
      }
      if(index !== -1){
        this.defaultItems.splice(index, 1);
      }
      this.render();
    },

    sortArrayListAZ: function(items) {
      items.models.sort(function(a, b){
          var name1 = (a.get('bucket')) ? a.get('bucket').toLowerCase() : "";
          var name2 = (b.get('bucket')) ? b.get('bucket').toLowerCase() : "";
          if(name1 === name2){
            return 0
          }
          return name1 < name2 ? -1 : name1 > name2 ? 1 : 0
      });
      return items;
    },

    sortArrayListHighLow: function(items) {
      items.models.sort(function(a, b){
        var x = parseFloat(a.get('value'));
        var y = parseFloat(b.get('value'));
        return x < y ? 1 : x > y ? -1 : 0
      });
      return items;
    },

    _addSwitch: function() {

      var self = this;

      this.switch = new cdb.forms.Switch({
        el: self.$el.find(".switch"),
        model: self.model,
        property: "list_view"
      });

    },

    _onKeyPress: function(e) {

      if (e.keyCode == 13) { this._onApply(e); }

    },

    _setFilter: function(t) {
      var m = new cdb.admin.models.CategoricalOperation({
        operator: 'AND',
        operation: 'EQUALS',
        text: t
      }, { collection: this.model.operations });
      this.model.operations.add(m);
    },

    _onApply: function(e) {

      e.preventDefault();
      e.stopPropagation();

      var $input = this.$input.find("input");
      var t = $input.attr("value");

      var m = new cdb.admin.models.CategoricalOperation({
        operator: 'OR',
        operation: 'BEGINSWITH',
        text: t
      }, { collection: this.model.operations });
      this.model.operations.add(m);

      // Reset input to blank
      $input.val("");
    },

    render: function() {
      var self = this;

      this.clearSubViews();

      var status = "loading";
      var count = this.model.get("count");

      if (count) {

        if ((this.model.items.length == 0 || (this.model.items.length == 1 && this.model.items.at(0).get("bucket") == null)) && !this.model.get('reached_limit')) {
          status = "empty";
        } else if (this.model.items.length == 1 && !this.model.get('reached_limit')) {
          status = "only";
        } else {
          status = "loaded";
        }

      }

      this.$el.html(this.getTemplate('table/menu_modules/filters/templates/selection')({
        fieldsToggle: this.fieldsHidden,
        sortFilterSelection: this.sortFilterSelection,
        listFilterValue: this.listFilterValue,
        listFilterValueRemote: this.listFilterValueRemote, 
        status: status,
        column_type: this.model.get("column_type"),
        legend: this.model.escape('column'),
        alias: this._getColumnAlias(),
        short_legend: this.model.escape('column'),
        list_view: true,
        reached_limit: this.model.get('reached_limit')
      }));

      this.$items     = this.$el.find('.items');
      this.$all       = this.$el.find('.all');
      this.$none      = this.$el.find('.none');
      this.$input     = this.$el.find('.input_field');
      this.$filter_counter     = this.$el.find('.filterCount');
      this.$filter_input       = this.$el.find('.filterInput');
      this.$filter_input_remote   = this.$el.find('.filterInput');
      this.$filter_remote      = this.$el.find('.filterInputRemote');
      this.$filterReset        = this.$el.find('.reset');
      this.$selectAll             = this.$el.find('.select_all');
      this.$filterInputReset   = this.$el.find('.clearFieldInput');
      //Input to search and filter existing list
      this.$filter_input.on('keyup', _.debounce(function (e) {
        var val = false;
        if(e.keyCode !== 40 && e.keyCode !== 38 && e.keyCode !== 13){
          if(this.$filter_input.val() && this.$filter_input.val().length > 0){
            this.listFilterValue = this.$filter_input.val();
            this.focusInputField = true;
            this.$filterInputReset.show();
            val = true;
          }else{
            this.$filter_input.removeClass('x');
            this.listFilterValue = null;
            this.$filterInputReset.hide();
          }
        this.render();
        this._setupClearX(val);
        }
      }.bind(this), 250));


      this._addItems();
      this._updateCounter();
      this._updateSortDisplay();

      if (this.focusInputField) {
        this.$filter_input.focus();
      }
      if (this.model.get('reached_limit')) {
        setTimeout(function () {
          this._setupAutoComplete();
        }.bind(this), 500);
      }
      if (this.toggle) {
        $(this.$el.find('.filterListandControls')).css({display: "none"});
      }
      if (this.totalCount > 0) {
         if (cdb.god.currentFilterLi) {
           cdb.god.currentFilterLi.removeClass('li-focus-item')
         }
         var element = this.$el.find('.items');
         if(element.length > 0){
          cdb.god.currentFilterLi = $(this.$el.find('.items')[0].firstChild);
          cdb.god.currentFilterLi.addClass('li-focus-item');
        }
       }
      return this;
    },

    _updateSortDisplay: function() {
      if(!this.userSelectedSort){
          if(this.model.get('reached_limit')){
            this.sortFilterSelection = "Default";
            this.$el.find(".sort-high-low").hide();
            this.$el.find(".sort-a-z").hide();
            this.$el.find(".sort-default").show();
          }else{
            this.sortFilterSelection = "HighLowAZ";
            this.$el.find(".sort-high-low").show();
            this.$el.find(".sort-a-z").hide();
            this.$el.find(".sort-default").hide();
          }
        }else{
          if(this.sortFilterSelection === "Default"){
            this.$el.find(".sort-high-low").hide();
            this.$el.find(".sort-a-z").hide();
            this.$el.find(".sort-default").show();
          }else if(this.sortFilterSelection === "HighLowAZ"){
            this.$el.find(".sort-high-low").show();
            this.$el.find(".sort-a-z").hide();
            this.$el.find(".sort-default").hide();
          }else{
            this.$el.find(".sort-high-low").hide();
            this.$el.find(".sort-a-z").show();
            this.$el.find(".sort-default").hide();
          }
        }
    },

    _setupClearX: function(val) {
      val ? this.$filter_input.addClass('x') : this.$filter_input.removeClass('x');
    },

    _renderOperations: function () {
      var self = this;
      this.$operations = this.$el.find('.operations');
      this.$operations.empty();

      this.model.operations.each(function (m) {
        var v = new OperationView({ model: m, column_type: self.model.get("column_type") });
        self.$operations.append(v.render().el);
        self.addView(v);
      });
    },

    _toggleSwitch: function() {

      if (!this.model.get("reached_limit")) {
        this.model.set("list_view", !this.model.get("list_view"));
      }

    },

    _updateInputFocus: function() {
      this.focusInputField = false;
    },

    _showFreeTextView: function() {

      this._toggleShadow("top", 0);
      this._toggleShadow("bottom", 0);

      this.$el.find(".scroll").animate({ height: 55 }, { duration: 250 });

      this.$input.fadeIn(250);
      this.$el.find(".operations").fadeIn(250);

      this.$el.find(".scroll .items").fadeOut(150);

      this._hideControllers();
      this._hideScrollers();

      this.$el.find(".fields").removeClass("list");
      this.$el.find(".fields").addClass("text");

      this.model.set("legend", this.model.escape('column'));

      this.trigger("refresh_scroll");

    },

    _showListView: function() {

      var count = this.model.get("count");

      this.$input.fadeOut(250);
      this.$el.find(".operations").fadeOut(250);

      this.$el.find(".scroll .items").fadeIn(150);
      var h = (this.$el.find(".scroll .items li").length + 1) * 34;

      this.$el.find(".scroll").animate({ height: h }, { duration: 150 });

      this._showControllers();

      var c = 0;

      if (count.selected)   c = count.selected;
      if (count.unselected) c += count.unselected;

      if (c >= 5) {
        this._showScrollers();
        this._toggleShadow("top", 0);
        this._toggleShadow("bottom", 1);
      } else {
        this._hideScrollers();
      }

      this.$el.find(".fields").removeClass("text");
      this.$el.find(".fields").addClass("list");

      this.model.set("legend", this.model.escape('column') + ":");

      this.trigger("refresh_scroll");


    },

    // Allows the selection of all or none of items
    _select: function(e) {

      e.preventDefault();
      e.stopPropagation();

      var $btn = $(e.target);

      if ($btn.hasClass("all")) this._selectAll(e);
      else this._selectNone(e);

      this._updateCounter();

    },

    _selectAll: function(e) {
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      this.$all.addClass("selected");
      this.$none.removeClass("selected");

      this.model.items.each(function (i) {
        i.set({ selected: true });
      });
      this.render();
      $(this.$el.find('.select_all')).hide();

    },

    _selectNone: function(e) {
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      this.$none.addClass("selected");
      this.$all.removeClass("selected");

      this.model.items.each(function(i) {
        i.set({ selected: false });
      });
      this.render();

    },

    _move: function (e) {

      e.preventDefault();
      e.stopPropagation();

      var $btn = $(e.target);

      if ($btn.hasClass("up")) this._moveUp(e);
      else this._moveDown(e);

    },

    // Turns shadows on/off
    _toggleShadow: function (pos, opacity) {
      this.$el.find(".white-gradient-shadow." + pos).animate({ opacity: opacity }, 50);
    },

    _moveUp: function (e) {
      var self = this;

      this._toggleShadow("bottom", 1);
      this.$el.find(".scrollers .down").removeClass("disabled");

      this.$scroll.stop().animate({ scrollTop: "-=" + this._SCROLLSTEP + "px" }, 150, function() {

        if (self.$items.position().top == 0) {
          self.$el.find(".scrollers .up").addClass("disabled");
          self._toggleShadow("top", 0);
        }

      });
    },

    _moveDown: function (e) {
      var self = this;

      this._toggleShadow("top", 1);

      this.$el.find(".scrollers .up").removeClass("disabled");

      this.$el.find(".scroll").stop().animate({ scrollTop: "+=" + this._SCROLLSTEP + "px" }, 150, function() {

        var scrollTopPos = self.$scroll.scrollTop();
        var listHeight   = self.$scroll.find("ul").height();
        var liHeight     = self.$scroll.find("li:last-child").outerHeight(true);

        if (scrollTopPos + self._SCROLLSTEP + liHeight >= listHeight) {
          self._toggleShadow("bottom", 0);
          self.$el.find(".scrollers .down").addClass("disabled");
        }

      });

    },

    _remove: function (e) {

      e.preventDefault();
      e.stopPropagation();

      this.model.destroy();
    },

    _getColumnAlias: function () {
      return (this.model.escape('alias')) ? this.model.escape('alias') : null;
    }
  });
})();
