# frozen-string-literal: true

module Sequel
  module Plugins
    # The paged_update_delete plugin adds +paged_update+ and
    # +paged_delete+ dataset methods.  These behave similarly to
    # the default +update+ and +delete+ dataset methods, except
    # that the update or deletion is done in potentially multiple
    # queries.  For a large table, this prevents the change from
    # locking the table for a long period of time.
    #
    # Because the point of this is to prevent locking tables for
    # long periods of time, the separate queries are not contained
    # in a transaction, which means if a later query fails,
    # earlier queries will still be committed.  You could prevent
    # this by using a transaction manually, but that defeats the
    # purpose of using these methods.
    #
    # Examples:
    #
    #   Album.where{name <= 'M'}.paged_update(:updated_at=>Sequel::CURRENT_TIMESTAMP)
    #   # SELECT id FROM albums WHERE (name <= 'M') ORDER BY id LIMIT 1 OFFSET 1001
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND ("id" < 1002))
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 1002)) ORDER BY id LIMIT 1 OFFSET 1001
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND ("id" < 1002) AND (id >= 1002))
    #   # ...
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 10002)) ORDER BY id LIMIT 1 OFFSET 1001
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND (id >= 10002))
    #
    #   Album.where{name > 'M'}.paged_delete
    #   # SELECT id FROM albums WHERE (name > 'M') ORDER BY id LIMIT 1 OFFSET 1001
    #   # DELETE FROM albums WHERE ((name > 'M') AND (id < 1002))
    #   # SELECT id FROM albums WHERE (name > 'M') ORDER BY id LIMIT 1 OFFSET 1001
    #   # DELETE FROM albums WHERE ((name > 'M') AND (id < 2002))
    #   # ...
    #   # SELECT id FROM albums WHERE (name > 'M') ORDER BY id LIMIT 1 OFFSET 10001
    #   # DELETE FROM albums WHERE (name > 'M')
    #
    # To set the number of rows to be updated or deleted per query
    # by +paged_update+ or +paged_delete+, you can use the
    # +paged_update_delete_size+ dataset method:
    #
    #   Album.where{name <= 'M'}.paged_update_delete_size(3).
    #     paged_update(:updated_at=>Sequel::CURRENT_TIMESTAMP)
    #   # SELECT id FROM albums WHERE (name <= 'M') ORDER BY id LIMIT 1 OFFSET 4
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND ("id" < 5))
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 5)) ORDER BY id LIMIT 1 OFFSET 4
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND ("id" < 9) AND (id >= 5))
    #   # ...
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 12345)) ORDER BY id LIMIT 1 OFFSET 4
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND (id >= 12345))
    # 
    # You should avoid using +paged_update+ with updates that
    # modify the primary key, as such usage is not supported by
    # this plugin.
    #
    # This plugin only supports models with scalar primary keys.
    #
    # Usage:
    #
    #   # Make all model subclasses support paged update/delete
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :paged_update_delete
    #
    #   # Make the Album class support paged update/delete
    #   Album.plugin :paged_update_delete
    module PagedUpdateDelete
      module ClassMethods
        Plugins.def_dataset_methods(self, [:paged_delete, :paged_update, :paged_update_delete_size])
      end

      module DatasetMethods
        # Delete all rows of the dataset using using multiple queries so that
        # no more than 1000 rows are deleted at a time (you can configure the
        # number of rows using paged_update_delete_size).
        def paged_delete
          pk = _paged_update_delete_pk(:paged_delete)
          rows_deleted = 0
          offset_ds = _paged_update_delete_offset_ds
          while last = offset_ds.get(pk)
            rows_deleted += where(pk < last).delete
          end
          rows_deleted + delete
        end

        # Update all rows of the dataset using using multiple queries so that
        # no more than 1000 rows are updated at a time (you can configure the
        # number of rows using paged_update_delete_size). All arguments are
        # passed to Dataset#update.
        def paged_update(*args)
          pk = _paged_update_delete_pk(:paged_update)
          rows_updated = 0
          base_offset_ds = offset_ds = _paged_update_delete_offset_ds
          first = nil

          while last = offset_ds.get(pk)
            ds = where(pk < last)
            ds = ds.where(pk >= first) if first
            rows_updated += ds.update(*args)
            first = last
            offset_ds = base_offset_ds.where(pk >= first)
          end
          ds = self
          ds = ds.where(pk >= first) if first
          rows_updated + ds.update(*args)
        end

        # Set the number of rows to update or delete per query when using
        # paged_update or paged_delete.
        def paged_update_delete_size(rows)
          raise Error, "paged_update_delete_size rows must be greater than 0" unless rows >= 1
          clone(:paged_updated_delete_rows=>rows)
        end

        private

        # Run some basic checks before running paged UPDATE or DELETE queries,
        # and return the primary key to operate on as a Sequel::Identifier.
        def _paged_update_delete_pk(meth)
          raise Error, "cannot use #{meth} if dataset has a limit or offset" if @opts[:limit] || @opts[:offset]

          case pk = model.primary_key
          when Symbol
            Sequel.identifier(pk)
          when Array
            raise Error, "cannot use #{meth} on a model with a composite primary key"
          else
            raise Error, "cannot use #{meth} on a model without a primary key"
          end
        end

        # The dataset that will be used by paged_update and paged_delete
        # to get the upper limit for the next UPDATE or DELETE query.
        def _paged_update_delete_offset_ds
          offset = @opts[:paged_updated_delete_rows] || 1000
          _force_primary_key_order.offset(offset+1)
        end
      end
    end
  end
end
