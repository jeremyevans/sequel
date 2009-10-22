module Sequel
  module Plugins
    # Sequel's built-in caching plugin supports caching to any object that
    # implements the Ruby-Memcache API.  You can add caching for any model
    # or for all models via:
    #
    #   Model.plugin :caching, store   # Cache all models
    #   MyModel.plugin :caching, store # Just cache MyModel
    #
    # The cache store should implement the Memcached API:
    # Note that the API is not the same as that of Ruby-MemCache or memcache-client. 
    # In particular, nil is a valid record value. 
    # Memcached#get does not return nil on failure, rather it raises Memcached::NotFound. 
    # This is consistent with the behavior of memcached itself. For example:
    
    # cache_store.set 'test', nil
    # cache_store.get 'test' #=> nil
    # cache_store.delete 'test'
    # cache_store.get 'test' #=> raises Memcached::NotFound

    module CachingMemcached
      # Set the cache_store and cache_ttl attributes for the given model.
      # If the :ttl option is not given, 3600 seconds is the default.
      def self.configure(model, store, opts={})
        model.instance_eval do
          @cache_store = store
          @cache_ttl = opts[:ttl] || 3600
        end
      end

      module ClassMethods
        # The cache store object for the model, which should implement the
        # Ruby-Memcache API
        attr_reader :cache_store
        
        # The time to live for the cache store, in seconds.
        attr_reader :cache_ttl

        # Set the time to live for the cache store, in seconds (default is 3600, # so 1 hour).
        def set_cache_ttl(ttl)
          @cache_ttl = ttl
        end
        
        # Copy the cache_store and cache_ttl to the subclass.
        def inherited(subclass)
          super
          store = @cache_store
          ttl = @cache_ttl
          subclass.instance_eval do
            @cache_store = store
            @cache_ttl = ttl
          end
        end

        private
    
        # Delete the entry with the matching key from the cache
        def cache_delete(key)
          @cache_store.delete(key)
          nil
        end
    
        # Return a key string for the pk
        def cache_key(pk)
          "#{self}:#{Array(pk).join(',')}"
        end
        
        # Check the cache before a database lookup unless a hash is supplied.
        def primary_key_lookup(pk)
          ck = cache_key(pk)
          begin 
            obj = @cache_store.get(ck)
          rescue
            if obj = super(pk)
              @cache_store.set(ck, obj, @cache_ttl)
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
