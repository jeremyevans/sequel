module Sequel
  module Plugins
    # The eager_each plugin makes calling each on an eager loaded dataset do eager loading.
    # By default, each does not work on an eager loaded dataset, because each iterates
    # over rows of the dataset as they come in, and to eagerly load you need to have all
    # values up front.  With the default associations code, you must call #all on an eagerly
    # loaded dataset, as calling #each on an #eager dataset skips the eager loading, and calling
    # #each on an #eager_graph dataset makes it yield plain hashes with columns from all
    # tables, instead of yielding the instances of the main model.
    #
    # This plugin makes #each call #all for eagerly loaded datasets.  As #all usually calls
    # #each, this is a bit of issue, but this plugin resolves the issue by cloning the dataset
    # and setting a new flag in the cloned dataset, so that each can check with the flag to
    # determine whether it should call all.
    #
    # Usage:
    #
    #   # Make all model subclass instances eagerly load for each (called before loading subclasses)
    #   Sequel::Model.plugin :eager_each
    #
    #   # Make the Album class eagerly load for each
    #   Album.plugin :eager_each
    module EagerEach
      # Methods added to eagerly loaded datasets when the eager_each plugin is in use.
      module EagerDatasetMethods
        # Call #all instead of #each unless #each is being called by #all.
        def each(&block)
          if opts[:all_called]
            super
          else
            all(&block)
          end
        end

        # Clone the dataset and set a flag to let #each know not to call #all,
        # to avoid the infinite loop.
        def all(&block)
          if opts[:all_called]
            super
          else
            clone(:all_called=>true).all(&block)
          end
        end
      end

      module DatasetMethods
        # Make sure calling each on this dataset will eagerly load the dataset.
        def eager(*)
          super.extend(EagerDatasetMethods)
        end

        # Make sure calling each on this dataset will eagerly load the dataset.
        def eager_graph(*)
          super.extend(EagerDatasetMethods)
        end
      end
    end
  end
end
