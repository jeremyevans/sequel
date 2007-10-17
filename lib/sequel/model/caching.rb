module Sequel
  class Model
    def self.set_cache(store, opts = {})
      @cache_store = store
      if (ttl = opts[:ttl])
        set_cache_ttl(ttl)
      end
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
  end
end