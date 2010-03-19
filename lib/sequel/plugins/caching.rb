module Sequel
  module Plugins
    # Sequel's built-in caching plugin supports caching to any object that
    # implements the Ruby-Memcache API (or memcached API with the :ignore_exceptions
    # option).  You can add caching for any model or for all models via:
    #
    #   Model.plugin :caching, store   # Cache all models
    #   MyModel.plugin :caching, store # Just cache MyModel
    #
    # The cache store should implement the Ruby-Memcache API:
    #
    #    cache_store.set(key, obj, time) # Associate the obj with the given key
    #                                    # in the cache for the time (specified
    #                                    # in seconds).
    #    cache_store.get(key) => obj     # Returns object set with same key.
    #    cache_store.get(key2) => nil    # nil returned if there isn't an object
    #                                    # currently in the cache with that key.
    #    cache_store.delete(key)         # Remove key from cache
    #
    # If the :ignore_exceptions option is true, exceptions raised by cache_store.get
    # are ignored and nil is returned instead.  The memcached API is to
    # raise an exception for a missing record, so if you use memcached, you will
    # want to use this option.
    #
    # Note that only Model.[] method calls with a primary key argument are cached
    # using this plugin.
    module Caching
      # Set the cache_store and cache_ttl attributes for the given model.
      # If the :ttl option is not given, 3600 seconds is the default.
      def self.configure(model, store, opts={})
        model.instance_eval do
          @cache_store = store
          @cache_ttl = opts[:ttl] || 3600
          @cache_ignore_exceptions = opts[:ignore_exceptions]
        end
      end

      module ClassMethods
        # If true, ignores exceptions when gettings cached records (the memcached API).
        attr_reader :cache_ignore_exceptions
        
        # The cache store object for the model, which should implement the
        # Ruby-Memcache (or memcached) API
        attr_reader :cache_store
        
        # The time to live for the cache store, in seconds.
        attr_reader :cache_ttl

        # Set the time to live for the cache store, in seconds (default is 3600, # so 1 hour).
        def set_cache_ttl(ttl)
          @cache_ttl = ttl
        end
        
        # Copy the necessary class instance variables to the subclass.
        def inherited(subclass)
          super
          store = @cache_store
          ttl = @cache_ttl
          cache_ignore_exceptions = @cache_ignore_exceptions
          subclass.instance_eval do
            @cache_store = store
            @cache_ttl = ttl
            @cache_ignore_exceptions = cache_ignore_exceptions
          end
        end

        private
    
        # Delete the entry with the matching key from the cache
        def cache_delete(ck)
          @cache_store.delete(ck)
          nil
        end
        
        def cache_get(ck)
          if @cache_ignore_exceptions
            @cache_store.get(ck) rescue nil
          else
            @cache_store.get(ck)
          end
        end
    
        # Return a key string for the pk
        def cache_key(pk)
          "#{self}:#{Array(pk).join(',')}"
        end
        
        # Set the object in the cache_store with the given key for cache_ttl seconds.
        def cache_set(ck, obj)
          @cache_store.set(ck, obj, @cache_ttl)
        end
        
        # Check the cache before a database lookup unless a hash is supplied.
        def primary_key_lookup(pk)
          ck = cache_key(pk)
          unless obj = cache_get(ck)
            if obj = super(pk)
              cache_set(ck, obj)
            end
          end 
          obj
        end
      end

      module InstanceMethods
        # Remove the object from the cache when updating
        def before_update
          return false if super == false
          cache_delete
        end

        # Return a key unique to the underlying record for caching, based on the
        # primary key value(s) for the object.  If the model does not have a primary
        # key, raise an Error.
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
    
        # Remove the object from the cache when deleting
        def delete
          cache_delete
          super
        end

        private
    
        # Delete this object from the cache
        def cache_delete
          model.send(:cache_delete, cache_key)
        end
      end
    end
  end
end
