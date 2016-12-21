# frozen-string-literal: true
#
# The freeze_datasets extension freezes a databases's datasets by
# default, and makes it so the databases's datasets are always
# frozen.  This makes sure you can never accidentally modify a
# dataset that may be used elsewhere (such as a model class's
# dataset or the same dataset being used in another thread).
# Frozen datasets can also perform caching for many
# different methods that can significantly improve performance.
#
# In addition to the caching provided by frozen datasets, this
# also adds caching of Database#from calls if the method is
# called with a single Symbol argument and not a block.  In
# addition to speeding up Dataset#from itself, because it
# returns a cached dataset, all caching done by that dataset
# can also improve performance.
#
# Usage:
#
#   DB.extension(:freeze_datasets)
#
# Related module: Sequel::FreezeDatasets

#
module Sequel
  class Database
    module FreezeDatasets
      module DatasetMethods
        # Make dup be an alias to clone, so that it
        # returns a frozen dataset.
        def dup
          clone
        end
      end

      # Reset the default dataset for this database after
      # loading the extension.
      def self.extended(db)
        db.extend_datasets(DatasetMethods)
      end

      # Cache returned dataset if given a single argument and no block.
      def from(*args, &block)
        if !block && args.length == 1 && (table = args[0]).is_a?(Symbol)
           @default_dataset.send(:cached_dataset, :"_from_#{table}_ds"){super}
        else
          super
        end
      end

      # Freeze datasets created from this dataset.
      def dataset
        super.freeze
      end

      private

      # Clear the cache of the default dataset when removing a cached
      # schema, in order to clear the from table cache.
      def remove_cached_schema(table)
        cache = @default_dataset.send(:cache)
        Sequel.synchronize{cache.clear}
        super
      end
    end

    register_extension(:freeze_datasets, FreezeDatasets)
  end
end
