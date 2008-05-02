module Sequel
  class Model
    metaattr_reader :cache_store, :cache_ttl

    # Set the cache store for the model
    def self.set_cache(store, opts = {})
      @cache_store = store
      @cache_ttl = opts[:ttl] || 3600
      before_save :cache_delete_unless_new
      before_update_values :cache_delete
      before_delete :cache_delete
    end
    
    # Set the time to live for the cache store
    def self.set_cache_ttl(ttl)
      @cache_ttl = ttl
    end
    
    ### Private Class Methods ###

    # Delete the entry with the matching key from the cache
    def self.cache_delete(key)
      @cache_store.delete(key)
      nil
    end

    # Return a key string for the pk
    def self.cache_key(pk)
      "#{self}:#{Array(pk).join(',')}"
    end

    # Lookup the primary key in the cache.
    # If found, return the matching object.
    # Otherwise, get the matching object from the database and
    # update the cache with it.
    def self.cache_lookup(pk) # :nodoc:
      ck = cache_key(pk)
      unless obj = @cache_store.get(ck)
        obj = dataset[primary_key_hash(pk)]
        @cache_store.set(ck, obj, cache_ttl)
      end
      obj
    end
    metaprivate :cache_delete, :cache_key, :cache_lookup

    # Return a key unique to the underlying record for caching
    def cache_key
      raise(Error, "No primary key is associated with this model") unless key = primary_key
      pk = case key
      when Array
        key.collect{|k| @values[k]}
      else
        @values[key] || (raise Error, 'no primary key for this record')
      end
      model.send(:cache_key, pk)
    end

    private
      # Delete this object from the cache
      def cache_delete
        model.send(:cache_delete, cache_key)
      end

      # Delete this object from the cache unless it is a new record
      def cache_delete_unless_new
        cache_delete unless new?
      end
  end
end
