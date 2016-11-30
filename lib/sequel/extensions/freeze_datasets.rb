# frozen-string-literal: true
#
# The freeze_datasets extension freezes a databases's datasets by
# default, and makes it so the databases's datasets are always
# frozen.  This makes sure you can never accidentally modify a
# dataset that may be used elsewhere (such as a model class's
# dataset or the same dataset being used in another thread).
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
        db.send(:reset_default_dataset)
      end

      # Freeze datasets created from this dataset.
      def dataset
        super.freeze
      end
    end

    register_extension(:freeze_datasets, FreezeDatasets)
  end
end
