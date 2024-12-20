# frozen-string-literal: true

module Sequel
  module Plugins
    # The static_cache_cache plugin allows for caching the row content for the current
    # class and subclasses that use the static_cache or subset_static_cache plugins.
    # Using this plugin can avoid the need to query the database every time loading
    # the static_cache plugin into a model (static_cache plugin) or using the
    # cache_subset method (subset_static_cache plugin).
    #
    # Usage:
    #
    #   # Make all model subclasses that use the static_cache plugin use
    #   # the cached values in the given file
    #   Sequel::Model.plugin :static_cache_cache, "static_cache.cache"
    #
    #   # Make the AlbumType model the cached values in the given file,
    #   # should be loaded before the static_cache plugin
    #   AlbumType.plugin :static_cache_cache, "static_cache.cache"
    module StaticCacheCache
      def self.configure(model, file)
        model.instance_variable_set(:@static_cache_cache_file, file)
        model.instance_variable_set(:@static_cache_cache, File.exist?(file) ? Marshal.load(File.read(file)) : {})
      end

      module ClassMethods
        # Dump the in-memory cached rows to the cache file.
        def dump_static_cache_cache
          File.open(@static_cache_cache_file, 'wb'){|f| f.write(Marshal.dump(sort_static_cache_hash(@static_cache_cache)))}
          nil
        end

        Plugins.inherited_instance_variables(self, :@static_cache_cache_file=>nil, :@static_cache_cache=>nil)

        private

        # Sort the given static cache hash in a deterministic way, so that 
        # the same static cache values will result in the same marshal file.
        def sort_static_cache_hash(cache)
          cache = cache.sort do |a, b|
            a, = a
            b, = b
            if a.is_a?(Array)
              if b.is_a?(Array)
                a_name, a_meth = a
                b_name, b_meth = b
                x = a_name <=> b_name
                if x.zero?
                  x = a_meth <=> b_meth
                end
                x
              else
                1
              end
            elsif b.is_a?(Array)
              -1
            else
              a <=> b
            end
          end
          Hash[cache]
        end

        # Load the rows for the model from the cache if available.
        # If not available, load the rows from the database, and
        # then update the cache with the raw rows.
        def load_static_cache_rows
          _load_static_cache_rows(dataset, name)
        end

        # Load the rows for the subset from the cache if available.
        # If not available, load the rows from the database, and
        # then update the cache with the raw rows.
        def load_subset_static_cache_rows(ds, meth)
          _load_static_cache_rows(ds, [name, meth].freeze)
        end

        # Check the cache first for the key, and return rows without a database
        # query if present.  Otherwise, get all records in the provided dataset,
        # and update the cache with them.
        def _load_static_cache_rows(ds, key)
          if rows = Sequel.synchronize{@static_cache_cache[key]}
            rows.map{|row| call(row)}.freeze
          else
            rows = ds.all.freeze
            raw_rows = rows.map(&:values)
            Sequel.synchronize{@static_cache_cache[key] = raw_rows}
            rows
          end
        end
      end
    end
  end
end
