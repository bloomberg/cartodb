# encoding: utf-8

class CommonDataRedisCache

  # This needs to be changed whenever there're changes in the code that require invalidation of old keys
  VERSION = '1'

  def initialize(redis_cache = $tables_metadata)
    @redis = redis_cache
  end

  def get(url)
    cache_key = key(url)
    value = redis.get(cache_key)
    if value.present?
      return JSON.parse(value, symbolize_names: true)
    else
      return nil
    end
  rescue Redis::BaseError => exception
    CartoDB.notify_exception(exception, { key: cache_key })
    nil
  end

  def set(url, response_headers, response_body)
    serialized = JSON.generate({headers: response_headers,
                                body: response_body
                               })
    redis.setex(key(url), 6.hours.to_i, serialized)
  rescue Redis::BaseError => exception
    CartoDB.notify_exception(exception, { key: key, headers: response_headers, body: response_body })
    nil
  end

  def invalidate
    keys = redis.keys("common_data:request:*")
    if !keys.empty?
      redis.del keys
    end
  rescue Redis::BaseError => exception
    CartoDB.notify_exception(exception)
    nil
  end

  def key(url)
    "common_data:request:#{url}:#{VERSION}"
  end

  private

  def redis
    @redis
  end

end
