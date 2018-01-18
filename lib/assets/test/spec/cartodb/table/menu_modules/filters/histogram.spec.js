var histogram = require('../../../../../../javascripts/cartodb/table/menu_modules/filters/histogram.js');
var abbreviateNumber = histogram.abbreviateNumber;

describe("abbreviateNumber", function() {
    
      it("should return 100 from 100", function() {
        var result = abbreviateNumber(100);
        expect(result).toEqual(100);
      });

      it("should return 101 from 100.5", function() {
        var result = abbreviateNumber(100.5);
        expect(result).toEqual(101);
      });

      it("should return 200 from 200", function() {
        var result = abbreviateNumber(200);
        expect(result).toEqual(200);
      });

      it("should return 500 from 500", function() {
        var result = abbreviateNumber(500);
        expect(result).toEqual(500);
      });

      it("should return 2.2k fom 2216.2", function() {
        var result = abbreviateNumber(2216.2);
        expect(result).toEqual("2.2k");
      });

      it("should return 2.2K from 2200", function() {
        var result = abbreviateNumber(2200);
        expect(result).toEqual("2.2k");
      });

      it("should return 5.6k from 5600", function() {
        var result = abbreviateNumber(5600);
        expect(result).toEqual("5.6k");
      });

      it("should return 56k from 56000", function() {
        var result = abbreviateNumber(56000);
        expect(result).toEqual("56k");
      });

      it("should return 7.3M from 7258540.24", function() {
        var result = abbreviateNumber(7258540.24);
        expect(result).toEqual("7.3M");
      });

      it("should return 67M from 66600000.24", function() {
        var result = abbreviateNumber(66600000.24);
        expect(result).toEqual("67M");
      });

      it("should return 6.7B from 6661234568.24", function() {
        var result = abbreviateNumber(6661234568.24);
        expect(result).toEqual("6.7B");
      });

      it("should return 24B from 23612345680.24", function() {
        var result = abbreviateNumber(23612345680.24);
        expect(result).toEqual("24B");
      });

      it("should return 0.3T from 256123456800.785", function() {
        var result = abbreviateNumber(256123456800.785);
        expect(result).toEqual("0.3T");
      });

      it("should return 6.6T from 6561234568000.785", function() {
        var result = abbreviateNumber(6561234568000.785);
        expect(result).toEqual("6.6T");
      });

      it("should return 43T from 42612345680000.29", function() {
        var result = abbreviateNumber(42612345680000.29);
        expect(result).toEqual("43T");
      });

      it("should return 0.6P from 566123456800000.29", function() {
        var result = abbreviateNumber(566123456800000.29);
        expect(result).toEqual("0.6P");
      });

      it("should return 2P from 2000000000000000", function() {
        var result = abbreviateNumber(2000000000000000);
        expect(result).toEqual("2P");
      });

      it("should return 2.4P from 2366123456800000.12547", function() {
        var result = abbreviateNumber(2366123456800000.12547);
        expect(result).toEqual("2.4P");
      });

      it("should return 71P from 71161234568000000.325487", function() {
        var result = abbreviateNumber(71161234568000000.325487);
        expect(result).toEqual("71P");
      });

      it("should return 0.7E from 711612345680000000.325487", function() {
        var result = abbreviateNumber(711612345680000000.325487);
        expect(result).toEqual("0.7E");
      });

      it("should return 2.5E from 2516123456800000000.325487", function() {
        var result = abbreviateNumber(2516123456800000000.325487);
        expect(result).toEqual("2.5E");
      });

      it("should return 34E from 2516123456800000000.325487", function() {
        var result = abbreviateNumber(34016123456800000000.325487);
        expect(result).toEqual("34E");
      });

      it("should return 0 from null", function() {
        var result = abbreviateNumber(null);
        expect(result).toEqual(0);
      });

      it("should return NaN from empty function call", function() {
        var result = abbreviateNumber();
        expect(result).toEqual(NaN);
      });

      it("should return o from empty empty string", function() {
        var result = abbreviateNumber('');
        expect(result).toEqual(0);
      });

});