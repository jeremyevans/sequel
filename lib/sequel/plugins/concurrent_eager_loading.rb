# frozen-string-literal: true

module Sequel
  extension 'async_thread_pool'

  module Plugins
    # The concurrent_eager_loading plugin allows for eager loading multiple associations
    # concurrently in separate threads.  You must load the async_thread_pool Database
    # extension into the Database object the model class uses in order for this plugin
    # to work.
    # 
    # By default in Sequel, eager loading happens in a serial manner.  If you have code
    # such as:
    #
    #   Album.eager(:artist, :genre, :tracks)
    #
    # Sequel will load the albums, then the artists for the albums, then
    # the genres for the albums, then the tracks for the albums.
    #
    # With the concurrent_eager_loading plugin, you can use the +eager_load_concurrently+
    # method to allow for concurrent eager loading:
    #
    #   Album.eager_load_concurrently.eager(:artist, :genre, :tracks)
    #
    # This will load the albums, first, since it needs to load the albums to know
    # which artists, genres, and tracks to eagerly load. However, it will load the
    # artists, genres, and tracks for the albums concurrently in separate threads.
    # This can significantly improve performance, especially if there is significant
    # latency between the application and the database. Note that using separate threads
    # is only used in the case where there are multiple associations to eagerly load.
    # With only a single association to eagerly load, there is no reason to use a
    # separate thread, since it would not improve performance.
    #
    # If you want to make concurrent eager loading the default, you can load the
    # plugin with the +:always+ option. In this case, all eager loads will be
    # concurrent.  If you want to force a non-concurrent eager load, you can use
    # +eager_load_serially+:
    #
    #   Album.eager_load_serially.eager(:artist, :genre, :tracks)
    #
    # Note that making concurrent eager loading the default is probably a bad idea
    # if you are eager loading inside transactions and want the eager load to
    # reflect changes made inside the transaction, unless you plan to use
    # +eager_load_serially+ for such cases.  See the async_thread_pool
    # Database extension documentation for more general caveats regarding its use.
    #
    # The default eager loaders for all of the association types that ship with Sequel
    # support safe concurrent eager loading.  However, if you are specifying a custom
    # +:eager_loader+ for an association, it may not work safely unless it it modified to
    # support concurrent eager loading.  Taking this example from the
    # {Advanced Associations guide}[rdoc-ref:doc/advanced_associations.rdoc]
    #
    #   Album.many_to_one :artist, eager_loader: (proc do |eo_opts|
    #     eo_opts[:rows].each{|album| album.associations[:artist] = nil}
    #     id_map = eo_opts[:id_map]
    #     Artist.where(id: id_map.keys).all do |artist|
    #       if albums = id_map[artist.id]
    #         albums.each do |album|
    #           album.associations[:artist] = artist
    #         end
    #       end
    #     end
    #   end)
    #
    # This would not support concurrent eager loading safely.  To support safe
    # concurrent eager loading, you need to make sure you are not modifying
    # the associations for objects concurrently by separate threads.  This is
    # implemented using a mutex, which you can access via <tt>eo_opts[:mutex]</tt>.
    # To keep things simple, you can use +Sequel.synchronize_with+ to only
    # use this mutex if it is available.  You want to use the mutex around the
    # code that initializes the associations (usually to +nil+ or <tt>[]</tt>),
    # and also around the code that sets the associatied objects appropriately
    # after they have been retreived.  You do not want to use the mutex around
    # the code that loads the objects, since that will prevent concurrent loading.
    # So after the changes, the custom eager loader would look like this:
    #
    #   Album.many_to_one :artist, eager_loader: (proc do |eo_opts|
    #     Sequel.synchronize_with(eo[:mutex]) do
    #       eo_opts[:rows].each{|album| album.associations[:artist] = nil}
    #     end
    #     id_map = eo_opts[:id_map]
    #     rows = Artist.where(id: id_map.keys).all
    #     Sequel.synchronize_with(eo[:mutex]) do
    #       rows.each do |artist|
    #         if albums = id_map[artist.id]
    #           albums.each do |album|
    #             album.associations[:artist] = artist
    #           end
    #         end
    #       end
    #     end
    #   end)
    #
    # Usage:
    #
    #   # Make all model subclass datasets support concurrent eager loading
    #   Sequel::Model.plugin :concurrent_eager_loading
    #
    #   # Make the Album class datasets support concurrent eager loading
    #   Album.plugin :concurrent_eager_loading
    #
    #   # Make all model subclass datasets concurrently eager load by default
    #   Sequel::Model.plugin :concurrent_eager_loading, always: true
    module ConcurrentEagerLoading
      def self.configure(mod, opts=OPTS)
        if opts.has_key?(:always)
          mod.instance_variable_set(:@always_eager_load_concurrently, opts[:always])
        end
      end

      module ClassMethods
        Plugins.inherited_instance_variables(self, :@always_eager_load_concurrently => nil)
        Plugins.def_dataset_methods(self, [:eager_load_concurrently, :eager_load_serially])

        # Whether datasets for this class should eager load concurrently by default.
        def always_eager_load_concurrently?
          @always_eager_load_concurrently
        end
      end

      module DatasetMethods
        # Return a cloned dataset that will eager load associated results concurrently
        # using the async thread pool.
        def eager_load_concurrently
          cached_dataset(:_eager_load_concurrently) do
            clone(:eager_load_concurrently=>true)
          end
        end

        # Return a cloned dataset that will noteager load associated results concurrently
        # using the async thread pool. Only useful if the current dataset has been marked
        # as loading concurrently, or loading concurrently is the model's default behavior.
        def eager_load_serially
          cached_dataset(:_eager_load_serially) do
            clone(:eager_load_concurrently=>false)
          end
        end

        private

        # Whether this particular dataset will eager load results concurrently.
        def eager_load_concurrently?
          v = @opts[:eager_load_concurrently]
          v.nil? ? model.always_eager_load_concurrently? : v
        end

        # If performing eager loads concurrently, and at least 2 associations are being
        # eagerly loaded, create a single mutex used for all eager loads.  After the
        # eager loads have been performed, force loading of any async results, so that
        # all eager loads will have been completed before this method returns.
        def perform_eager_loads(eager_load_data)
          return super if !eager_load_concurrently? || eager_load_data.length < 2

          mutex = Mutex.new
          eager_load_data.each_value do |eo|
            eo[:mutex] = mutex
          end

          super.each do |v|
            if Sequel::Database::AsyncThreadPool::BaseProxy === v
              v.__value
            end
          end
        end

        # If performing eager loads concurrently, perform this eager load using the
        # async thread pool.
        def perform_eager_load(loader, eo)
          eo[:mutex] ? db.send(:async_run){super} : super
        end
      end
    end
  end
end
