(function() {

  /*
  * Filter's model
  *
  **/

  cdb.admin.mod.Filter = cdb.core.View.extend({

    _SHORT_MONTH_NAMES: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    _MONTH_NAMES: ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'],

    tagName: 'li',
    className: 'filter histogram',

    events: {
      'click a.remove'              :  '_remove',
      'focus  .range-number input'  :  '_onNumberFilterFocused',
      'blur   .range-number input'  :  '_hideNumberField',
      'change .range-number input'  :  '_onNumberFilterChanged',
      'click  .toggleButton'        :  '_toggle',
      'click  .range-text'          :  '_showRangeInput',
      'click  .range-date-text'     :  '_showDateRangeInput',
      'click  .date-min'            :  '_showCalenderMin',
      'click  .date-max'            :  '_showCalenderMax',
      'focus  .date-max'            :  '_showCalenderMax',
      'click  .ui-close-calendar'   :  '_closeUICalendar',
      'click  .ui-open-calendar'    :  '_openUICalendar',
      'blur   .date-max'            :  '_removeFocusDateMax',
      'blur   .date-min'            :  '_removeFocusDateMin'
    },

    initialize: function() {

      _.bindAll(this, "_barChart");

      this.model.bind('change:hist',  this.render,  this);
      this.model.bind('change:lower', this._renderRange, this);
      this.model.bind('change:upper', this._renderRange, this);
      this.model.bind('error',        this._checkEmpty,  this);
      this.toggle = this.model.get('toggle');

      this.isDate = (this.model.get("column_type") == 'date');
      this.isNumber = (this.model.get("column_type") == 'number');
      this.calOpen = false;
      this.calInputSelect = null;
      if (this.isDate) {
        this.$el.addClass('date');
      }
      cdb.god.on('closeDialogs', function(){
        if(
            this.$el.find('.range-number .max').is(':focus') ||
            this.$el.find('.range-number .min').is(':focus') ||
            this.calOpen || this.brush
          )
          {
            if(this.brush){
              this.brush = false;
            }
            return
         }
        this.render();
      }.bind(this));
      this._setupCalender();
      this.$calDateMin = this.$el.find('.range .date-min');
      this.$calDateMax = this.$el.find('.range .date-max');
    },

    _removeFocusDateMax: function(){
       $(this.$el.find('.range .date-max')).removeClass('has-focus');
       if(!this.calOpen){
         var regex = /(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])\/\d{4}/;
         if(regex.test(this.$el.find('.range .date-max').val())){
           var inputDate = this.$el.find('.range .date-max').val();
           var inputDateArray = inputDate.split('/');
           var modelDate = this.model.get('lower');
           var dateObj = new Date(inputDateArray[2], inputDateArray[0] -1, inputDateArray[1]).getTime();
           if(inputDate !== dateObj){
             if(dateObj > this.model.get('upper_limit')){
               dateObj = this.model.get('upper_limit');
               this.$el.find('.range .date-max').val(new Date(this.model.get('upper_limit')).toLocaleDateString());
             }
             this.model.set('upper', dateObj);
             this._renderHist();
           }
         }else{
           this.$el.find('.range .date-max').val(new Date(this.model.get('upper')).toLocaleDateString());
         }
       }else{
         this.calender.hide();
         this.$el.find('.ui-close-calendar').hide();
         this.$el.find('.ui-open-calendar').show();
         this.calOpen= false;
       }
     },

     _removeFocusDateMin: function(){
       if(!this.calOpen){
         var regex = /(0?[1-9]|1[012])\/(0?[1-9]|[12][0-9]|3[01])\/\d{4}/;
         if(regex.test(this.$el.find('.range .date-min').val())){
           var inputDate = this.$el.find('.range .date-min').val();
           var inputDateArray = inputDate.split('/');
           var modelDate = this.model.get('lower');
           var dateObj = new Date(inputDateArray[2], inputDateArray[0] -1, inputDateArray[1]).getTime();
           if(inputDate !== dateObj){
             if(dateObj < this.model.get('lower_limit')){
               dateObj = this.model.get('lower_limit');
               this.$el.find('.range .date-min').val(new Date(this.model.get('lower_limit')).toLocaleDateString());
             }
             this.model.set('lower', dateObj);
             this._renderHist();
           }
         }else{
           this.$el.find('.range .date-min').val(new Date(this.model.get('lower')).toLocaleDateString());
         }
       }
     },



    _renderHist: function() {
      var self = this;

      var histData = this.model.get('hist');

      if(!histData) return;

      if (histData.length == 1) {
        var M = histData.length;

        this.$el.find(".legend").html(this._cleanString(this.model.escape('column'), 15) + ":");

        this.$el.find(".loading").hide();
        this.$el.find(".empty").hide();
        this.$el.find(".range").hide();
        this.$el.find(".range-number").hide();
        this.$el.find(".only").fadeIn(150);

        return;
      }

      this._checkEmpty();
      this._setupCalender();
      var data = histData.map(function(x, i) {
        return { value: x };
      });

      var filter = crossfilter(data);

      var dim = function(k) {
        return filter.dimension(function (d) {
          return d[k];
        });
      }

      var def = function(k) {

        var dimK  = dim(k);

        var width = 332;

        var lower = self.model.get("lower");
        var upper = self.model.get("upper");

        // Inverse interpolation
        var span = self.model.get("upper_limit") - self.model.get("lower_limit")
        var bar_size = span/data.length;
        var l = (lower - self.model.get("lower_limit")) * data.length / span;
        var u = (upper - self.model.get("lower_limit")) * data.length / span;

        return self._barChart()
        .dimension(dimK)
        .group(data.map(function(d) { return parseFloat(d.value, 10); }))
        .round(function(v) {
          return Math.ceil(v);
        })
        .x(
          d3.scale.linear()
          .domain([0, data.length])
          .range([0, width])
        )
        .filter([l, u])
      }

      var chartDefs = [def('value')];

      var chartDivs = d3.select(".hist." + this.cid)
      .data(chartDefs)
      .each(function(chartDiv) {
        chartDiv.on("brush", renderAll).on("brushend", renderAll);
      });

      function renderAll() {
        chartDivs.each(function(method) {
          d3.select(this).call(method);
        });
      }
      renderAll();

    },

    _closeUICalendar: function(e) {
      e.preventDefault();
      e.stopPropagation();
      this.calOpen = false;
      this.calender.hide();
      this.$el.find('.ui-close-calendar').hide();
      $(this.$el.find('.ui-open-calendar')).show();
      this._renderHist();
      $(this.$el.find('.range .date-max')).removeClass('has-focus');
      $(this.$el.find('.range .date-min')).removeClass('has-focus');
    },

    _openUICalendar: function(e) {
       e.preventDefault();
       e.stopPropagation();
       this.calOpen = true;
       this._showCalenderMin();
       $(this.$el.find('.ui-open-calendar')).hide();
       $(this.$el.find('.range .date-min')).focus();
     },

    _setupCalender: function() {
      this.$el.find(".ui-datepicker-div").hide();
       this.calender = this.$el.find(".ui-datepicker-div")
           .datepicker({
               changeMonth: true,
               changeYear: true,
               rangeSelect: true,
               numberOfMonths: 1,
               onSelect: function(dateText, inst) {
                   if(this.calInputSelect === 'max'){
                    this.model.set('upper', new Date(dateText).getTime());
                    this.calender.hide();
                    this.calOpen = false;
                    this.$el.find('.ui-close-calendar').hide();
                    $(this.$el.find('.range .date-max')).removeClass('has-focus');
                    $(this.$el.find('.range .date-min')).removeClass('has-focus');
                    $(this.$el.find('.ui-open-calendar')).show();
                    this._renderHist();
                  }else{
                    this.model.set('lower', new Date(dateText).getTime());
                    this.$el.find('.date-max').focus();
                  }
                }.bind(this),
               beforeShow: function(input) {
               },
               beforeShowDay: function (date) {
                 if(this.$el.find('.date-min').val() && this.$el.find('.date-max').val()){
                  if(new Date(this.$el.find('.date-min').val()) < date && date < new Date(this.$el.find('.date-max').val())){
                    return [true, "ui-state-highlight"];
                  }else{
                    return [true, ""];
                   }
                  }else{
                    return [true, ""];
                  }
                }.bind(this)
             });
     },
    _showCalenderMax: function(e){
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      this.calInputSelect = 'max';
      this.calender.datepicker('setDate', new Date(this.$el.find('.date-max').val()));
      if (this.calOpen) {
        this.calender.show();
        this.$el.find('.ui-close-calendar').show();
      }
      $(this.$el.find('.range .date-max')).addClass('has-focus');
      $(this.$el.find('.range .date-min')).removeClass('has-focus');
    },

    _showCalenderMin: function(e) {
      if (e) {
        e.preventDefault();
        e.stopPropagation();
      }
      this.calInputSelect = 'min';
      this.calender.datepicker('setDate', new Date(this.$el.find('.date-min').val()));
      if (this.calOpen) {
        this.calender.show();
        this.$el.find('.ui-close-calendar').show();
      }
      $(this.$el.find('.range .date-max')).removeClass('has-focus');
      $(this.$el.find('.range .date-min')).addClass('has-focus');
    },

    _hideCalender: function(){
      if(this.calOpen){
      this.calOpen = false;
      this.openCal = false;
      this.calender.hide();
      this.$el.find('.ui-close-calendar').hide();
      }
    },
 
     _showDateRangeInput: function(e) {
       e.preventDefault();
       e.stopPropagation();
       this.$el.find('.range').show();
       this.$el.find('.range-date-text').hide();
       $(this.$el.find('.range .date-min')).addClass('has-focus');
       $(this.$el.find('.range .date-min')).focus();
     },

    render: function() {

      var self = this;

      this.$el.html(this.getTemplate('table/menu_modules/filters/templates/histogram')({
        legend: this.model.escape('column'),
        alias: this._getColumnAlias(),
        cid: self.cid
      }));
      this.$el.find('.ui-close-calendar').hide();
      this._renderHist();
      if(this.toggle){
        $(this.$el.find('.hist.' + this.cid)).css({display: "none"});
      }
      return this;
    },

    _hideNumberField: function(e){
      setTimeout(function(){
        if(this.$el.find('.range-number .max').is(':focus') || this.$el.find('.range-number .min').is(':focus')){
          return
        }else{
          this.$el.find('.range-number').hide();
          this.$el.find('.range-text').show();
        }
      }.bind(this), 200);
    },

    _showRangeInput: function(e){
      e.preventDefault();
      e.stopPropagation();
      this.$el.find('.range-number').show();
      this.$el.find('input.min').focus();
      this.$el.find('.range-text').hide();
    },

    _cleanString: function(s, n) {

      if (s) {
        s = s.replace(/<(?:.|\n)*?>/gm, ''); // strip HTML tags
        s = s.substr(0, n-1) + (s.length > n ? '&hellip;' : ''); // truncate string
      }

      return s;

    },

     _toggle: function(e) {
      e.preventDefault();
      e.stopPropagation();
      this.toggle = !this.toggle;
      this.model.set('toggle', this.toggle);
      $(this.$el.find('.hist.' + this.cid)).toggle();
    },

    _checkEmpty: function() {

      var self = this;

      setTimeout(function() {

        var hist = self.model.get("hist");

        if (hist) {

          if (hist.length > 1) {

            self.$(".empty").hide();
            self.$(".loading").hide();
            self.$(".only").hide();
            if (this.isNumber) {
              self.$(".range-number").fadeIn(250);
            }
          } else {

            self.$el.find(".legend").html(self.model.escape('column'));

            self.$(".loading").hide();
            self.$(".range").hide();
            self.$(".range-number").hide();
            self.$(".empty").hide();
            self.$(".only").fadeIn(150);
          }
        } else {

          self.$el.find(".legend").html(self._cleanString(self.model.escape('column'), 25) + ":");

          self.$(".range").hide();
          self.$(".range-number").hide();
          self.$(".loading").hide();
          self.$(".empty").fadeIn(150);
        }}
      , 250);

      },

    _getMinMaxFromDate: function(lower, upper) {

      var min, max;

      lower = Math.round(lower);
      upper = Math.round(upper);

      var min_date = this.model._getDateFromTimestamp(lower);
      var max_date = this.model._getDateFromTimestamp(upper);

      var minMaxDate = this._formatDate(min_date, max_date);

      return { min: minMaxDate.min, max: minMaxDate.max };

    },

    _formatLowerUpper: function(lower, upper) {
      return { min: lower.toPrecision( Math.round(lower).toString().length + 2), max: upper.toPrecision( Math.round(upper).toString().length + 2) };
    },


    _getColumnAlias: function () {
      if (this.model.table.get('synchronization') && this.model.table.get('synchronization').from_external_source) {
        var alias = this.model.escape('alias');
        if (alias) { return this.model.escape('alias'); }

        return null;
      }
      return (this.model.escape('alias')) ? this.model.escape('alias') : null;
    },

    _renderRange: function() {

      var lower = this.model.get('lower');
      var upper = this.model.get('upper');
      if (_.isNaN(upper) || _.isNaN(lower)) { return; }

      var minMax = {};

      if (this.isDate) {
        minMax = this._getMinMaxFromDate(lower, upper);
      } else {
        minMax = this._formatLowerUpper(lower, upper);
      }
      this.dateRange = minMax;
      this.upper = upper;
      this.lower = lower;
      this._renderRangeText(upper, lower, minMax);
    },

    calculateNumericDisplayVlaue: function(number) {
      var correctedNumber = null;
      if(number > 99 || number < -100){
        correctedNumber = parseFloat(number).toFixed(0);
      }else if (number < 1 || number > -1){
        correctedNumber = parseFloat(number).toFixed(2);
      }else if(number > 1000){
        correctedNumber = number;
      }else{
        correctedNumber = parseFloat(number).toFixed(2);
      }
      return correctedNumber;
    },

    _renderRangeText: function (upper, lower, minMax) {

      if (!_.isNaN(upper) && !_.isNaN(lower)) {
        if (this.isNumber) {
          var minVal = this.calculateNumericDisplayVlaue(minMax.min);
          var maxVal = this.calculateNumericDisplayVlaue(minMax.max);
          if(minVal > 999 || minVal < -999){
            minVal = this.nFormatter(parseFloat(minVal), 0);
          }
          if(maxVal > 999 || maxVal < -999){
            maxVal = this.nFormatter(parseFloat(maxVal), 0);
          }
          this.$el.find('.range-date-text').hide();
          this.$el.find('.range-number input.min').val(minMax.min);
          this.$el.find('.range-number input.max').val(minMax.max);
          this.$el.find('.range-text').html(minVal + ' - ' + maxVal);
        } else {
          this.$el.find('.range-number').hide();
          this.$el.find('.range-date-text').html(minMax.min + " - " + minMax.max);
          this.$el.find('.date-min').val(minMax.min);
          this.$el.find('.date-max').val(minMax.max);
          this.calender.datepicker( "option", "maxDate", new Date(this.model.getUpperLimit()));
          this.calender.datepicker( "option", "minDate", new Date(this.model.getLowerLimit()));
        }
      }
    },

    _onNumberFilterFocused: function () {
        this.$el.find('.range-number .subtext').show();
    },

    _onNumberFilterChanged: function (event) {
        var $input = $(event.target);
        var value = parseFloat($input.val());
        var isMin = $input.hasClass('min');
        var modelKey = isMin ? 'lower' : 'upper';
        if (!isNaN(value)) {
          var lower = this.model.get('lower');
          var upper = this.model.get('upper');
          var lowerLimit = this.model.get('lower_limit');
          var upperLimit = this.model.get('upper_limit');
          if (value < lowerLimit) { value = lowerLimit; }
          if (value > upperLimit) { value = upperLimit; }
          // Swap lower/upper if input value is in lower and is greater than upper
          if (isMin && value > upper) {
            this.model.set('upper', value);
            value = upper;
          }
          // Swap lower/upper if input value is in upper and is less than lower
          if (!isMin && value < lower) {
            this.model.set('lower', value);
            value = lower;
          }
          this.model.set(modelKey, value);
          this._renderHist();
        } else {
          $input.val(this.model.get(modelKey));
        }
        this.$el.find('.range-number .subtext').hide();
    },

    _remove: function(e) {
      e.preventDefault();
      e.stopPropagation();

      this.model.destroy();
    },

    _updateBounds:function(bounds, update) {

      if (bounds) {

        var n = this.model.get("hist").length;
        var lower = this.model.interpolate(bounds[0]/n);
        var upper = this.model.interpolate(bounds[1]/n);

        if (update) {
          if (!_.isNaN(lower)) this.model.set('lower', lower);
          if (!_.isNaN(upper)) this.model.set('upper', upper);
        }

        var minMax = null;
        if (this.isDate) {
          minMax = this._getMinMaxFromDate(lower, upper);
        } else {
          minMax = this._formatLowerUpper(lower, upper);
        }

        this._renderRangeText(upper, lower, minMax);
      }

    },

    _formatDate: function(min_date, max_date) {

      function get_nth_suffix(date) {
        switch (date) {
          case 1:
          case 21:
          case 31:
            return 'st';
          case 2:
          case 22:
            return 'nd';
          case 3:
          case 23:
            return 'rd';
          default:
            return 'th';
        }
      }

      function pad(n, width, z) {
        z = z || '0';
        n = n + '';
        return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
      }

      var minDay = min_date.getDate();
      var maxDay = max_date.getDate();

      var minYear = min_date.getFullYear();
      var maxYear = max_date.getFullYear();

      var minMonth = min_date.getMonth();
      var maxMonth = max_date.getMonth();

      var minTime = pad(min_date.getHours(), 2) + ":" + pad(min_date.getMinutes(), 2) + " ";
      var minDate = min_date.toLocaleDateString();

      var maxTime = pad(max_date.getHours(), 2) + ":" + pad(max_date.getMinutes(), 2) + " ";
      var maxDate = max_date.toLocaleDateString();

      return { min: minDate, max: maxDate}

    },  

    nFormatter: function (num, digits) {
      var neg = false;
      if(num < 0){
        num = Math.abs(num);
        neg = true;
      }
      var si = [
        { value: 1E18, symbol: "e" },
        { value: 1E15, symbol: "p" },
        { value: 1E12, symbol: "t" },
        { value: 1E9,  symbol: "b" },
        { value: 1E6,  symbol: "m" },
        { value: 1E3,  symbol: "k" }
      ], rx = /\.0+$|(\.[0-9]*[1-9])0+$/, i;
      for (i = 0; i < si.length; i++) {
        if (num >= si[i].value) {
          if(neg){
            return "-" + (num / si[i].value).toFixed(digits).replace(rx, "$1") + si[i].symbol;
          }
          return (num / si[i].value).toFixed(digits).replace(rx, "$1") + si[i].symbol;
        }
      }
      if(neg){
        return "-" + num.toFixed(digits).replace(rx, "$1");
      }
      return num.toFixed(digits).replace(rx, "$1");
    },

    _calculateTicksNum: function(x){
      var axis = d3.svg.axis().scale(x).orient("bottom");
      var ticks = [1, 2, 3, 4, 5];
      return axis.tickValues(ticks).tickFormat(function(d, idx) {
        var bucketSize = ((this.model.getUpperLimit() - this.model.getLowerLimit())/4);
        switch (idx) {
          case 0:
            var value = this.calculateNumericDisplayVlaue(this.model.getLowerLimit());
            if(value > 999 || value < -999){
              value = this.nFormatter(parseFloat(value), 0);
            }
            return value;
          case 1:
            var value = this.calculateNumericDisplayVlaue(bucketSize + this.model.getLowerLimit());
            if(value > 999 || value < -999){
              value = this.nFormatter(parseFloat(value), 0);
            }
            return value;
          case 2:
            var value = this.calculateNumericDisplayVlaue(bucketSize * 2 + this.model.getLowerLimit());
            if(value > 999 || value < -999){
              value = this.nFormatter(parseFloat(value), 0);
            }
            return value;
          case 3:
            var value = this.calculateNumericDisplayVlaue(bucketSize * 3 + this.model.getLowerLimit());
            if(value > 999 || value < -999){
              value = this.nFormatter(parseFloat(value), 0);
            }
            return value;
          case 4:
            var value = this.calculateNumericDisplayVlaue(this.model.getUpperLimit());
            if(value > 999 || value < -999){
              value = this.nFormatter(parseFloat(value), 0);
            }
            return value;
          }
      }.bind(this));
    },

    _getAxis: function(x) {
      if(this.isNumber){
          return this._calculateTicksNum(x);
      }else if(this.isDate){
          return this._calculateTicksDate(x);
      }
    },

    _calculateTicksDate: function(x){
      var axis = d3.svg.axis().scale(x).orient("bottom");
      var ticks = [1, 2, 3, 4, 5];
      var bucketSize = 332/4;
      return axis.tickValues(ticks).tickFormat(function(d, idx) {
        var bucketSize = (this.getUpperLimit() - this.getLowerLimit())/4;
        switch (idx) {
          case 0:
            return (new Date(this.getLowerLimit()).toLocaleDateString());
          case 1:
            return (new Date(bucketSize + this.getLowerLimit()).toLocaleDateString());
          case 2:
            return (new Date(bucketSize * 2 + this.getLowerLimit()).toLocaleDateString());
          case 3:
            return (new Date(bucketSize * 3 + this.getLowerLimit()).toLocaleDateString());
          case 4:
            return new Date(this.getUpperLimit()).toLocaleDateString();
          }
      }.bind(this.model));
    },

    _barChart: function() {

      var self = this;

      var minHeight     = 97,
          margin = {top: 0, right: 10, bottom: 0, left: 10},
          x             = d3.scale.ordinal().domain([1, 2, 3, 4, 5]).rangePoints([0, 332]),
          x_date        = d3.scale.ordinal().domain([1, 2, 3, 4, 5]).rangePoints([0, 332]),
          y   = d3.scale.linear().range([100, 0]),
          id     = this.cid,
          brush  = d3.svg.brush(),
          brushDirty,
          dimension,
          group,
          round,
          initialBounds = [];

      function chart(div) {

        var
          width  = x.range()[ x.range().length -1 ],
          height = y.range()[0];

        y.domain([0, d3.max(group)]);

        div.each(function(el, idx) {

          var div = d3.select(this),
          g = div.select("g");

        // Create the skeletal chart.
        if (g.empty()) {

          g = div.append("svg")
          .attr("style", "padding:3px")
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom + 25)
          .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        g.append("clipPath")
          .attr("id", "clip-" + id)
          .append("rect")
          .attr("width", width)
          .attr("height", height);

        g.selectAll(".bar")
          .data(['background', 'foreground'])
          .enter().append("path")
          .attr("class", function(d) { return d + " bar"; })
          .data([group, group])
          if (this.isNumber) {
            var xAxis = self._getAxis(x);
          } else {
            var xAxis = self._getAxis(x_date);
          }
          g.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(function(a) {
              return xAxis(a)
          });

          g.selectAll(".foreground.bar")
          .attr("clip-path", "url(#clip-" + id + ")");

          g.selectAll("text")
            .attr("fill", "#d8d8d8");

        // Initialize the brush component with pretty resize handles.
        var gBrush = g.append("g").attr("class", "brush").call(brush);
        gBrush.selectAll("rect").attr("height", height);
        gBrush.selectAll(".resize").append("path").attr("d", resizePath);
        }

        // Only redraw the brush if set externally.
        if (brushDirty) {

          brushDirty = false;
          g.selectAll(".brush").call(brush);

          if (brush.empty()) {
            g.selectAll("#clip-" + id + " rect")
              .attr("x", 0)
              .attr("width", width);
          } else {

            var extent = brush.extent();

            g.selectAll("#clip-" + id + " rect")
              .attr("x", x(extent[0]))
              .attr("width", x(extent[1]) - x(extent[0]));


            self._updateBounds(extent, false);

          }
        }

        g.selectAll(".bar").attr("d", barPath);
          if (self.isDate) {
            var txt = g.selectAll(".x text");
            txt[0][0].setAttribute("x", 15);
            txt[0][ txt[0].length - 1 ].setAttribute("x", -15);
          }
        });

        function barPath(h, i) {

          var path = [],
              i = -1,
              n = h.length,
              d;

          var barWidth = width/n;
          while (++i < n) {
            d = h[i];

            inverseHeight = y(d);
            if (inverseHeight > minHeight && inverseHeight < height ) inverseHeight = minHeight;
            path.push("M", x(i), ",", height, "V", inverseHeight, "h", barWidth, "V", height);
          }

          return path.join("");

        }

        function resizePath(d) {
          var e = +(d == "e"),
              x = e ? 1 : -1,
              y = height / 3;
          return "M" + (.5 * x) + "," + y
            + "A6,6 0 0 " + e + " " + (6.5 * x) + "," + (y + 6)
            + "V" + (2 * y - 6)
            + "A6,6 0 0 " + e + " " + (.5 * x) + "," + (2 * y)
            + "Z"
            + "M" + (2.5 * x) + "," + (y + 8)
            + "V" + (2 * y - 8)
            + "M" + (4.5 * x) + "," + (y + 8)
            + "V" + (2 * y - 8);
        }
      }

      brush.on("brushstart.chart", function() {
        self.brush = true;
        var extent = brush.extent();
        initialBounds = [extent];
        self._updateBounds(extent, false);
      });

      brush.on("brush.chart", function() {

        var extent = brush.extent();

        if (initialBounds.length >= 2) {
          if (extent[0] === initialBounds[1][0]) {
            extent[0] = initialBounds[0][0];
          } else if (extent[1] === initialBounds[1][1]) {
            extent[1] = initialBounds[0][1];
          }

          var g = d3.select(this.parentNode);
          g.select("#clip-" + id + " rect")
            .attr("x", x(extent[0]))
            .attr("width", x(extent[1]) - x(extent[0]));

          self._updateBounds(extent, false);
        } else {
          initialBounds.push(extent);
        }

      });

      brush.on("brushend.chart", function() {
        var extent = brush.extent();
        if (initialBounds.length >= 2) {
          if (extent[0] === initialBounds[1][0]) {
            extent[0] = initialBounds[0][0];
          } else if (extent[1] === initialBounds[1][1]) {
            extent[1] = initialBounds[0][1];
          }

          brush.extent(extent);
          self._updateBounds(extent, true);
        }
      });

      chart.margin = function(_) {
        if (!arguments.length) return margin;
        margin = _;
        return chart;
      };

      chart.x = function(_) {
        if (!arguments.length) return x;
        x = _;
        brush.x(x);
        return chart;
      };

      chart.y = function(_) {
        if (!arguments.length) return y;
        y = _;
        return chart;
      };

      chart.dimension = function(_) {
        if (!arguments.length) return dimension;
        dimension = _;
        return chart;
      };

      chart.filter = function(_) {
        if (_) {
          brush.extent(_);
          dimension.filterRange(_);
        } else {
          brush.clear();
          dimension.filterAll();
        }
        brushDirty = true;
        return chart;
      };

      chart.group = function(_) {
        if (!arguments.length) return group;
        group = _;
        return chart;
      };

      chart.round = function(_) {
        if (!arguments.length) return round;
        round = _;
        return chart;
      };

      return d3.rebind(chart, brush, "on");
    }

  });

})();
