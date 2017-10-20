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

      url.get_param('a').should eq ['1']
      url.get_param(:a).should eq ['1']
      url.get_param('a').should eq url.get_param(:a)

      url.get_param('b').should eq ['2']
      url.get_param(:b).should eq ['2']
      url.get_param('b').should eq url.get_param(:b)

      url.get_params['a'].should eq ['1']
      url.get_params[:a].should eq ['1']
      url.get_params['a'].should eq url.get_params[:a]

      url.get_params['b'].should eq ['2']
      url.get_params[:b].should eq ['2']
      url.get_params['b'].should eq url.get_params[:b]
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

    it 'returns deep copy of array of values and not a modifiable reference' do
      url = Carto::Url.new('ftp://abc:123@baz.com:456/?a=1&b=2&c=3&a=4')
      a = url.get_param(:a)
      a.should eq %w{1 4}

      s = a.first
      s[0] = '3'
      a << '5'
      url.get_param(:a).should eq %w{1 4}
    end

    it 'returns deep copy of params hash from get_params' do
      url = Carto::Url.new('ftp://abc:123@baz.com:456/?a=1&b=2&c=3&a=4')
      ps1 = url.get_params

      ps1[:a].should eq ['1', '4']
      ps1[:b].should eq ['2']
      ps1[:c].should eq ['3']

      # Modify value array
      a = ps1[:a]
      a << '5'
      a.should eq %w{1 4 5}
      url.get_params[:a].should eq %w{1 4}

      # Modify value array element
      s = ps1[:b].first
      s[0] = '3'
      url.get_params[:b].should eq ['2']

      # Overwrite array
      ps1[:c] = ['5']
      url.get_params[:c].should eq ['3']
    end
  end
end
