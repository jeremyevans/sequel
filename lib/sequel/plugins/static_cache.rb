module Sequel
  module Plugins
    # The static_cache plugin is designed for models that are not modified at all
    # in production use cases, or at least where modifications to them would usually
    # coincide with an application restart.  When loaded into a model class, it 
    # retrieves all rows in the database and staticly caches a ruby array and hash
    # keyed on primary key containing all of the model instances.  All of these instances
    # are frozen so they won't be modified unexpectedly.
    #
    # The caches this plugin creates are used for the following things:
    #
    # * Primary key lookups (e.g. Model[1])
    # * Model.all calls
    # * Model.each calls
    # * Model.map calls without an argument
    # * Model.to_hash calls without an argument
    #
    # Usage:
    #
    #   # Cache the AlbumType class staticly
    #   AlbumType.plugin :static_cache
    module StaticCache
      # Populate the static caches when loading the plugin.
      def self.configure(model)
        model.send(:load_cache)
      end

      module ClassMethods
        # A frozen ruby hash holding all of the model's frozen instances, keyed by frozen primary key.
        attr_reader :cache

        # An array of all of the model's frozen instances, without issuing a database
        # query.
        def all
          @all.dup
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
          cache[pk]
        end

        # Yield each of the model's frozen instances to the block, without issuing a database
        # query.
        def each(&block)
          @all.each(&block)
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
          else
            @all.map(&(Proc.new if block_given?))
          end
        end

        Plugins.after_set_dataset(self, :load_cache)

        # Use the cache instead of a query to get the results.
        def to_hash(key_column = nil, value_column = nil)
        return cache.dup if key_column.nil? && value_column.nil?

        h = {}
        if value_column
          if value_column.is_a?(Array)
            if key_column.is_a?(Array)
              each{|r| h[r.values.values_at(*key_column)] = r.values.values_at(*value_column)}
            else
              each{|r| h[r[key_column]] = r.values.values_at(*value_column)}
            end
          else
            if key_column.is_a?(Array)
              each{|r| h[r.values.values_at(*key_column)] = r[value_column]}
            else
              each{|r| h[r[key_column]] = r[value_column]}
            end
          end
        elsif key_column.is_a?(Array)
          each{|r| h[r.values.values_at(*key_column)] = r}
        else
          each{|r| h[r[key_column]] = r}
        end
        h
        end

        # Use the cache instead of a query to get the results
        def to_hash_groups(key_column, value_column = nil)
          h = {}
          if value_column
            if value_column.is_a?(Array)
              if key_column.is_a?(Array)
                each{|r| (h[r.values.values_at(*key_column)] ||= []) << r.values.values_at(*value_column)}
              else
                each{|r| (h[r[key_column]] ||= []) << r.values.values_at(*value_column)}
              end
            else
              if key_column.is_a?(Array)
                each{|r| (h[r.values.values_at(*key_column)] ||= []) << r[value_column]}
              else
                each{|r| (h[r[key_column]] ||= []) << r[value_column]}
              end
            end
          elsif key_column.is_a?(Array)
            each{|r| (h[r.values.values_at(*key_column)] ||= []) << r}
          else
            each{|r| (h[r[key_column]] ||= []) << r}
          end
          h
        end

        private

        # Return the frozen object with the given pk, or nil if no such object exists
        # in the cache, without issuing a database query.
        def primary_key_lookup(pk)
          cache[pk]
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
      end
    end
  end
end
