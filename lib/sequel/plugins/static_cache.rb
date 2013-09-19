module Sequel
  module Plugins
    # The static_cache plugin is designed for models that are not modified at all
    # in production use cases, or at least where modifications to them would usually
    # coincide with an application restart.  When loaded into a model class, it
    # retrieves all rows in the database and statically caches a ruby array and hash
    # keyed on primary key containing all of the model instances.  All of these instances
    # are frozen so they won't be modified unexpectedly, and before hooks disallow
    # saving or destroying instances.
    #
    # You can use the :frozen=>false option to have this plugin return unfrozen
    # instances.  This is slower as it requires creating new objects, but it allows
    # you to make changes to the object and save them.  If you set the option to false,
    # you are responsible for updating the cache manually (the pg_static_cache_updater
    # extension can handle this automatically).
    #
    # The caches this plugin creates are used for the following things:
    #
    # * Primary key lookups (e.g. Model[1])
    # * Model.all
    # * Model.each
    # * Model.count (without an argument or block)
    # * Model.map
    # * Model.to_hash
    # * Model.to_hash_groups
    #
    # Usage:
    #
    #   # Cache the AlbumType class statically, disallowing any changes.
    #   AlbumType.plugin :static_cache
    #
    #   # Cache the AlbumType class statically, but return unfrozen instances
    #   # that can be modified.
    #   AlbumType.plugin :static_cache, :frozen=>false
    module StaticCache
      # Populate the static caches when loading the plugin. Options:
      # :frozen :: Whether retrieved model objects are frozen.  The default is true,
      #            for better performance as the shared frozen objects can be used
      #            directly.  If set to false, new instances are created.
      def self.configure(model, opts=OPTS)
        model.instance_eval do
          @static_cache_frozen = opts.fetch(:frozen, true)
          load_cache
        end
      end

      module ClassMethods
        # A frozen ruby hash holding all of the model's frozen instances, keyed by frozen primary key.
        attr_reader :cache

        # An array of all of the model's frozen instances, without issuing a database
        # query.
        def all
          if @static_cache_frozen
            @all.dup
          else
            map{|o| o}
          end
        end

        # Get the number of records in the cache, without issuing a database query.
        def count(*a, &block)
          if a.empty? && !block
            @all.size
          else
            super
          end
        end

        # Return the frozen object with the given pk, or nil if no such object exists
        # in the cache, without issuing a database query.
        def cache_get_pk(pk)
          static_cache_object(cache[pk])
        end

        # Yield each of the model's frozen instances to the block, without issuing a database
        # query.
        def each(&block)
          if @static_cache_frozen
            @all.each(&block)
          else
            @all.each{|o| yield(static_cache_object(o))}
          end
        end

        # Use the cache instead of a query to get the results.
        def map(column=nil, &block)
          if column
            raise(Error, "Cannot provide both column and block to map") if block
            if column.is_a?(Array)
              @all.map{|r| r.values.values_at(*column)}
            else
              @all.map{|r| r[column]}
            end
          elsif @static_cache_frozen
            @all.map(&block)
          elsif block
            @all.map{|o| yield(static_cache_object(o))}
          else
            all.map
          end
        end

        Plugins.after_set_dataset(self, :load_cache)
        Plugins.inherited_instance_variables(self, :@static_cache_frozen=>nil)

        # Use the cache instead of a query to get the results.
        def to_hash(key_column = nil, value_column = nil)
        if key_column.nil? && value_column.nil?
          if @static_cache_frozen
            return cache.dup
          else
            key_column = primary_key
          end
        end

        h = {}
        if value_column
          if value_column.is_a?(Array)
            if key_column.is_a?(Array)
              @all.each{|r| h[r.values.values_at(*key_column)] = r.values.values_at(*value_column)}
            else
              @all.each{|r| h[r[key_column]] = r.values.values_at(*value_column)}
            end
          else
            if key_column.is_a?(Array)
              @all.each{|r| h[r.values.values_at(*key_column)] = r[value_column]}
            else
              @all.each{|r| h[r[key_column]] = r[value_column]}
            end
          end
        elsif key_column.is_a?(Array)
          @all.each{|r| h[r.values.values_at(*key_column)] = static_cache_object(r)}
        else
          @all.each{|r| h[r[key_column]] = static_cache_object(r)}
        end
        h
        end

        # Use the cache instead of a query to get the results
        def to_hash_groups(key_column, value_column = nil)
          h = {}
          if value_column
            if value_column.is_a?(Array)
              if key_column.is_a?(Array)
                @all.each{|r| (h[r.values.values_at(*key_column)] ||= []) << r.values.values_at(*value_column)}
              else
                @all.each{|r| (h[r[key_column]] ||= []) << r.values.values_at(*value_column)}
              end
            else
              if key_column.is_a?(Array)
                @all.each{|r| (h[r.values.values_at(*key_column)] ||= []) << r[value_column]}
              else
                @all.each{|r| (h[r[key_column]] ||= []) << r[value_column]}
              end
            end
          elsif key_column.is_a?(Array)
            @all.each{|r| (h[r.values.values_at(*key_column)] ||= []) << static_cache_object(r)}
          else
            @all.each{|r| (h[r[key_column]] ||= []) << static_cache_object(r)}
          end
          h
        end

        # Ask whether modifications to this class are allowed.
        def static_cache_allow_modifications?
          !@static_cache_frozen
        end

        private

        # Return the frozen object with the given pk, or nil if no such object exists
        # in the cache, without issuing a database query.
        def primary_key_lookup(pk)
          static_cache_object(cache[pk])
        end

        # Reload the cache for this model by retrieving all of the instances in the dataset
        # freezing them, and populating the cached array and hash.
        def load_cache
          a = dataset.all
          h = {}
          a.each{|o| h[o.pk.freeze] = o.freeze}
          @all = a.freeze
          @cache = h.freeze
        end

        # If :frozen=>false is not used, just return the argument. Otherwise,
        # create a new instance with the arguments values if the argument is
        # not nil.
        def static_cache_object(o)
          if @static_cache_frozen
            o
          elsif o
            call(o.values.dup)
          end
        end
      end

      module InstanceMethods
        # Disallowing destroying the object unless the :frozen=>false option was used.
        def before_destroy
          return false unless model.static_cache_allow_modifications?
          super
        end

        # Disallowing saving the object unless the :frozen=>false option was used.
        def before_save
          return false unless model.static_cache_allow_modifications?
          super
        end
      end
    end
  end
end
