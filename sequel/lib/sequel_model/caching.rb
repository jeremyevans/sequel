module Sequel
  class Error
    class CacheNotFound < Error; end
  end
end

module Sequel
  class Model
    def self.set_cache(store, opts = {})
      @cache_store = store
      if (ttl = opts[:ttl])
        set_cache_ttl(ttl)
      end
      
      meta_def(:[]) do |*args|
        if (args.size == 1) && (Hash === (h = args.first))
          return dataset[h]
        end
        
        begin
          obj = @cache_store.get(cache_key_from_values(args))
          raise Sequel::Error::CacheNotFound unless obj
        rescue Exception => e
          if e.is_a? Sequel::Error::CacheNotFound or
            (defined? Memcached and e.is_a? Memcached::NotFound)
            obj = dataset[primary_key_hash((args.size == 1) ? args.first : args)]
            @cache_store.set(cache_key_from_values(args), obj, cache_ttl)
          else
            raise
          end
        end

        obj
      end

      class_def(:update_values){|v| self.class.cache_del(cache_key); super }
      class_def(:save){ self.class.cache_del(cache_key) unless new?; super }
      class_def(:delete){ self.class.cache_del(cache_key); super }
    end
    
    def self.set_cache_ttl(ttl)
      @cache_ttl = ttl
    end
    
    def self.cache_store
      @cache_store
    end
    
    def self.cache_ttl
      @cache_ttl ||= 3600
    end

    def self.cache_del key
      begin
        cache_store.delete(key)
      rescue Exception => e
        raise unless defined? Memcached and e.is_a? Memcached::NotFound
      end
    end
    
    def self.cache_key_from_values(values)
      "#{self}:#{values.join(',')}"
    end
  end
end
