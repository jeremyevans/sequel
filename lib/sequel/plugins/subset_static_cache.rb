# frozen-string-literal: true

module Sequel
  module Plugins
    # The subset_static_cache plugin is designed for model subsets that are not modified at all
    # in production use cases, or at least where modifications to them would usually
    # coincide with an application restart.  When caching a model subset, it 
    # retrieves all rows in the database and statically caches a ruby array and hash
    # keyed on primary key containing all of the model instances.  All of these cached
    # instances are frozen so they won't be modified unexpectedly.
    #
    # With the following code:
    #
    #   class StatusType < Sequel::Model
    #     dataset_module do
    #       where :available, hidden: false
    #     end
    #     cache_subset :available
    #   end
    #   
    # The following methods will use the cache and not issue a database query:
    #
    # * StatusType.available.with_pk
    # * StatusType.available.all
    # * StatusType.available.each
    # * StatusType.available.first (without block, only supporting no arguments or single integer argument)
    # * StatusType.available.count (without an argument or block)
    # * StatusType.available.map
    # * StatusType.available.as_hash
    # * StatusType.available.to_hash
    # * StatusType.available.to_hash_groups
    #
    # The cache is not used if you chain methods before or after calling the cached
    # method, as doing so would not be safe:
    #
    #   StatusType.where{number > 1}.available.all 
    #   StatusType.available.where{number > 1}.all
    #
    # The cache is also not used if you change the class's dataset after caching
    # the subset, or in subclasses of the model.
    #
    # You should not modify any row that is statically cached when using this plugin,
    # as otherwise you will get different results for cached and uncached method
    # calls.
    module SubsetStaticCache
      def self.configure(model)
        model.class_exec do
          @subset_static_caches ||= ({}.compare_by_identity)
        end
      end

      module ClassMethods
        # Cache the given subset statically, so that calling the subset method on
        # the model will return a dataset that will return cached results instead
        # of issuing database queries (assuming the cache has the necessary
        # information).
        #
        # The model must already respond to the given method before cache_subset
        # is called.
        def cache_subset(meth)
          ds = send(meth).with_extend(CachedDatasetMethods)
          cache = ds.instance_variable_get(:@cache)

          rows, hash = subset_static_cache_rows(ds, meth)
          cache[:subset_static_cache_all] = rows
          cache[:subset_static_cache_map] = hash

          caches = @subset_static_caches
          caches[meth] = ds
          model = self
          subset_static_cache_module.send(:define_method, meth) do
            if (model == self) && (cached_dataset = caches[meth])
              cached_dataset
            else
              super()
            end
          end
          nil
        end

        Plugins.after_set_dataset(self, :clear_subset_static_caches)
        Plugins.inherited_instance_variables(self, :@subset_static_caches=>proc{{}.compare_by_identity})

        private

        # Clear the subset_static_caches.  This is used if the model dataset
        # changes, to prevent cached values from being used.
        def clear_subset_static_caches
          @subset_static_caches.clear
        end

        # A module for the subset static cache methods, so that you can define
        # a singleton method in the class with the same name, and call super
        # to get default behavior.
        def subset_static_cache_module
          return @subset_static_cache_module if @subset_static_cache_module

          # Ensure dataset_methods module is defined and class is extended with
          # it before calling creating this module.
          dataset_methods_module

          mod_name = "#{name}::@subset_static_cache_module"
          Sequel.synchronize{@subset_static_cache_module ||= Sequel.set_temp_name(Module.new){mod_name}}
          extend(@subset_static_cache_module)
          @subset_static_cache_module
        end
         
        # Return the frozen array and hash used for caching the subset
        # of the given dataset.
        def subset_static_cache_rows(ds, meth)
          all = load_subset_static_cache_rows(ds, meth)
          h = {}
          all.each do |o|
            o.errors.freeze
            h[o.pk.freeze] = o.freeze
          end
          [all, h.freeze]
        end

        # Return a frozen array for all rows in the dataset.
        def load_subset_static_cache_rows(ds, meth)
          ret = super if defined?(super)
          ret || ds.all.freeze
        end
      end

      module CachedDatasetMethods
        # An array of all of the dataset's instances, without issuing a database
        # query. If a block is given, yields each instance to the block.
        def all(&block)
          return super unless all = @cache[:subset_static_cache_all]

          array = all.dup
          array.each(&block) if block
          array
        end

        # Get the number of records in the cache, without issuing a database query,
        # if no arguments or block are provided.
        def count(*a, &block)
          if a.empty? && !block && (all = @cache[:subset_static_cache_all])
            all.size
          else
            super
          end
        end

        # If a block is given, multiple arguments are given, or a single
        # non-Integer argument is given, performs the default behavior of
        # issuing a database query.  Otherwise, uses the cached values
        # to return either the first cached instance (no arguments) or an
        # array containing the number of instances specified (single integer
        # argument).
        def first(*args)
          if !defined?(yield) && args.length <= 1 && (args.length == 0 || args[0].is_a?(Integer)) && (all = @cache[:subset_static_cache_all])
            all.first(*args)
          else
            super
          end
        end

        # Return the frozen object with the given pk, or nil if no such object exists
        # in the cache, without issuing a database query.
        def with_pk(pk)
          if cache = @cache[:subset_static_cache_map]
            cache[pk]
          else
            super
          end
        end

        # Yield each of the dataset's frozen instances to the block, without issuing a database
        # query.
        def each(&block)
          return super unless all = @cache[:subset_static_cache_all]
          all.each(&block)
        end

        # Use the cache instead of a query to get the results.
        def map(column=nil, &block)
          return super unless all = @cache[:subset_static_cache_all]
          if column
            raise(Error, "Cannot provide both column and block to map") if block
            if column.is_a?(Array)
              all.map{|r| r.values.values_at(*column)}
            else
              all.map{|r| r[column]}
            end
          else
            all.map(&block)
          end
        end

        # Use the cache instead of a query to get the results if possible
        def as_hash(key_column = nil, value_column = nil, opts = OPTS)
          return super unless all = @cache[:subset_static_cache_all]

          if key_column.nil? && value_column.nil?
            if opts[:hash]
              key_column = model.primary_key
            else
              return Hash[@cache[:subset_static_cache_map]]
            end
          end

          h = opts[:hash] || {}
          if value_column
            if value_column.is_a?(Array)
              if key_column.is_a?(Array)
                all.each{|r| h[r.values.values_at(*key_column)] = r.values.values_at(*value_column)}
              else
                all.each{|r| h[r[key_column]] = r.values.values_at(*value_column)}
              end
            else
              if key_column.is_a?(Array)
                all.each{|r| h[r.values.values_at(*key_column)] = r[value_column]}
              else
                all.each{|r| h[r[key_column]] = r[value_column]}
              end
            end
          elsif key_column.is_a?(Array)
            all.each{|r| h[r.values.values_at(*key_column)] = r}
          else
            all.each{|r| h[r[key_column]] = r}
          end
          h
        end

        # Alias of as_hash for backwards compatibility.
        def to_hash(*a)
          as_hash(*a)
        end

        # Use the cache instead of a query to get the results
        def to_hash_groups(key_column, value_column = nil, opts = OPTS)
          return super unless all = @cache[:subset_static_cache_all]

          h = opts[:hash] || {}
          if value_column
            if value_column.is_a?(Array)
              if key_column.is_a?(Array)
                all.each{|r| (h[r.values.values_at(*key_column)] ||= []) << r.values.values_at(*value_column)}
              else
                all.each{|r| (h[r[key_column]] ||= []) << r.values.values_at(*value_column)}
              end
            else
              if key_column.is_a?(Array)
                all.each{|r| (h[r.values.values_at(*key_column)] ||= []) << r[value_column]}
              else
                all.each{|r| (h[r[key_column]] ||= []) << r[value_column]}
              end
            end
          elsif key_column.is_a?(Array)
            all.each{|r| (h[r.values.values_at(*key_column)] ||= []) << r}
          else
            all.each{|r| (h[r[key_column]] ||= []) << r}
          end
          h
        end
      end
    end
  end
end
