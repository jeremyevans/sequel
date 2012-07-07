module Sequel
  module Plugins
    # The sharding plugin augments Sequel's default model sharding support
    # in the following ways:
    #
    # 1) It automatically sets model instances to be saved back to the
    #    shard they were retreived from.
    # 2) It makes model associations use the same shard as the model
    #    object.
    # 3) It adds a slightly nicer API for creating model instances on
    #    specific shards.
    #
    # Usage:
    #
    #   # Add the sharding support to all model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :sharding
    #
    #   # Add the sharding support to the Album class
    #   Album.plugin :sharding
    module Sharding
      module ClassMethods
        # Create a new object on the given shard s.
        def create_using_server(s, values={}, &block)
          new_using_server(s, values, &block).save
        end

        # When eagerly loading, if the current dataset has a defined shard and the
        # dataset that you will be using to get the associated records does not,
        # use the current dataset's shard for the associated dataset.
        def eager_loading_dataset(opts, ds, select, associations, eager_options={})
          ds = super(opts, ds, select, associations, eager_options)
          if !ds.opts[:server] and s = eager_options[:self] and server = s.opts[:server]
            ds = ds.server(server)
          end
          ds
        end

        # Return a newly instantiated object that is tied to the given
        # shard s.  When the object is saved, a record will be inserted
        # on shard s.
        def new_using_server(s, values={}, &block)
          new(values, &block).set_server(s)
        end

        private

        # Set the server for each graphed dataset to the current server
        # unless the graphed dataset already has a server set.
        def eager_graph_dataset(opts, eager_options)
          ds = super
          if s = eager_options[:self].opts[:server]
            ds = ds.server(s) unless ds.opts[:server]
          end
          ds
        end
      end

      module InstanceMethods
        # Set the server that this object is tied to, unless it has
        # already been set.  Returns self.
        def set_server?(s)
          @server ||= s
          self
        end

        private

        # Ensure that association datasets are tied to the correct shard.
        def _apply_association_options(*args)
          use_server(super)
        end

        # Ensure that the join table for many_to_many associations uses the correct shard.
        def _join_table_dataset(opts)
          use_server(super)
        end

        # If creating the object by doing <tt>add_association</tt> for a
        # +many_to_many+ association, make sure the associated object is created on the
        # current object's shard, unless the passed object already has an assigned shard.
        def ensure_associated_primary_key(opts, o, *args)
          o.set_server?(@server) if o.respond_to?(:set_server?)
          super
        end
      end

      module DatasetMethods
        # If a row proc exists on the dataset, replace it with one that calls the
        # previous row_proc, but calls set_server on the output of that row_proc,
        # ensuring that objects retrieved by a specific shard know which shard they
        # are tied to.
        def server(s)
          ds = super
          if rp = row_proc
            ds.row_proc = proc{|r| rp.call(r).set_server(s)}
          end
          ds
        end
      end
    end
  end
end
