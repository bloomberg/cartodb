require_relative '../../spec_helper'
require_relative '../../../lib/carto/url'

module Carto
  describe('Url') do
    it 'accepts only valid uris' do
      # Valid
      expect { Carto::Url.new('foo') }.to_not raise_error
      expect { Carto::Url.new('foo/') }.to_not raise_error
      expect { Carto::Url.new('/foo/') }.to_not raise_error
      expect { Carto::Url.new('/foo/?a=1&b=2') }.to_not raise_error
      expect { Carto::Url.new('http://foo.com/foo/?a=1&b=2') }.to_not raise_error
      expect { Carto::Url.new('ssh://foo.com/foo/?a=1&b=2') }.to_not raise_error
      expect { Carto::Url.new('ftp://foo.com/foo/?a=1&b=2') }.to_not raise_error

      # Invalid
      expect { Carto::Url.new('http://foo.com/ /') }.to raise_error
      expect { Carto::Url.new('http://foo.com/%') }.to raise_error
    end

    it 'supports indifferent access' do
      url = Carto::Url.new('http://abc.com?a=1&b=2')

      (url.get_param('a') == ['1']).should be_true
      (url.get_param(:a) == ['1']).should be_true
      (url.get_param('a') == url.get_param(:a)).should be_true

      (url.get_param('b') == ['2']).should be_true
      (url.get_param(:b) == ['2']).should be_true
      (url.get_param('b') == url.get_param(:b)).should be_true

      (url.get_params['a'] == ['1']).should be_true
      (url.get_params[:a] == ['1']).should be_true
      (url.get_params['a'] == url.get_params[:a]).should be_true

      (url.get_params['b'] == ['2']).should be_true
      (url.get_params[:b] == ['2']).should be_true
      (url.get_params['b'] == url.get_params[:b]).should be_true
    end

    it 'returns new array as default key' do
      url = Carto::Url.new('https://abc.com/?a=1')
      params = url.get_params
      foo = params['foo']

      url.get_param(:foo).should eq []
      url.get_param('foo').should eq []
      foo.should eq []
      params['foo'] << 'bar'
      foo.should eq []
      params['foo'].should eq []
      params[:foo].should eq []
    end

    it 'returns the same values with get_params and get_param' do
      url = Carto::Url.new('ftp://abc:123@baz.com:456/?a=1&b=2&c=3&a=4')
      params = url.get_params

      params[:a].should eq ['1', '4']
      params[:b].should eq ['2']
      params[:c].should eq ['3']

      params.keys.each do |k|
        params[k].should eq url.get_param(k)
        params[k].should eq url.get_param(k.to_sym)
        params[k.to_sym].should eq url.get_param(k)
        params[k.to_sym].should eq url.get_param(k.to_sym)
      end
    end
  end
end
