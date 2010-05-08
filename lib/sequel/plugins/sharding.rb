module Sequel
  module Plugins
    # The sharding plugin makes it easy to use Sequel's sharding features
    # with models.  It lets you create model objects on specific shards,
    # and any models retrieved from specific shards are automatically
    # saved back to those shards.  It also works with associations,
    # so that model objects retrieved from specific shards will only
    # return associated objects from that shard, and using the
    # add/remove/remove_all association methods will only affect
    # that shard.
    module Sharding
      module ClassMethods
        # Create a new object on the given shard s.
        def create_using_server(s, values={}, &block)
          new_using_server(s, values, &block).save
        end

        # Return a newly instantiated object that is tied to the given
        # shard s.  When the object is saved, a record will be inserted
        # on shard s.
        def new_using_server(s, values={}, &block)
          new(values, &block).set_server(s)
        end
      end

      module InstanceMethods
        # Set the shard that this object is tied to.  Returns self.
        def set_server(s)
          @server = s
          self
        end

        # Set the server that this object is tied to, unless it has
        # already been set.  Returns self.
        def set_server?(s)
          @server ||= s
          self
        end

        # Ensure that the instance dataset is tied to the correct shard.
        def this
          use_server(super)
        end

        private

        # Ensure that association datasets are tied to the correct shard.
        def _apply_association_options(*args)
          use_server(super)
        end

        # Ensure that the object is inserted into the correct shard.
        def _insert_dataset
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

        # Set the given dataset to use the current object's shard.
        def use_server(ds)
          @server ? ds.server(@server) : ds
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
