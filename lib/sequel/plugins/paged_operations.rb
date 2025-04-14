# frozen-string-literal: true

module Sequel
  module Plugins
    # The paged_operations plugin adds +paged_update+ and
    # +paged_delete+ dataset methods.  These behave similarly to
    # the default +update+ and +delete+ dataset methods, except
    # that the update or deletion is done in potentially multiple
    # queries (by default, affecting 1000 rows per query).
    # For a large table, this prevents the change from
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
    #   Album.where{name <= 'M'}.paged_update(updated_at: Sequel::CURRENT_TIMESTAMP)
    #   # SELECT id FROM albums WHERE (name <= 'M') ORDER BY id LIMIT 1 OFFSET 1000
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND ("id" < 1002))
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 1002)) ORDER BY id LIMIT 1 OFFSET 1000
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND ("id" < 2002) AND (id >= 1002))
    #   # ...
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 10002)) ORDER BY id LIMIT 1 OFFSET 1000
    #   # UPDATE albums SET updated_at = CURRENT_TIMESTAMP WHERE ((name <= 'M') AND (id >= 10002))
    #
    #   Album.where{name > 'M'}.paged_delete
    #   # SELECT id FROM albums WHERE (name > 'M') ORDER BY id LIMIT 1 OFFSET 1000
    #   # DELETE FROM albums WHERE ((name > 'M') AND (id < 1002))
    #   # SELECT id FROM albums WHERE (name > 'M') ORDER BY id LIMIT 1 OFFSET 1000
    #   # DELETE FROM albums WHERE ((name > 'M') AND (id < 2002))
    #   # ...
    #   # SELECT id FROM albums WHERE (name > 'M') ORDER BY id LIMIT 1 OFFSET 1000
    #   # DELETE FROM albums WHERE (name > 'M')
    #
    # The plugin also adds a +paged_datasets+ method that will yield
    # separate datasets limited in size that in total handle all
    # rows in the receiver:
    #
    #   Album.where{name > 'M'}.paged_datasets{|ds| puts ds.sql}
    #   # Runs: SELECT id FROM albums WHERE (name <= 'M') ORDER BY id LIMIT 1 OFFSET 1000
    #   # Prints: SELECT * FROM albums WHERE ((name <= 'M') AND ("id" < 1002))
    #   # Runs: SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 1002)) ORDER BY id LIMIT 1 OFFSET 1000
    #   # Prints: SELECT * FROM albums WHERE ((name <= 'M') AND ("id" < 2002) AND (id >= 1002))
    #   # ...
    #   # Runs: SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 10002)) ORDER BY id LIMIT 1 OFFSET 1000
    #   # Prints: SELECT * FROM albums WHERE ((name <= 'M') AND (id >= 10002))
    #
    # To set the number of rows per page, pass a :rows_per_page option:
    #
    #   Album.where{name <= 'M'}.paged_update({x: Sequel[:x] + 1}, rows_per_page: 4)
    #   # SELECT id FROM albums WHERE (name <= 'M') ORDER BY id LIMIT 1 OFFSET 4
    #   # UPDATE albums SET x = x + 1 WHERE ((name <= 'M') AND ("id" < 5))
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 5)) ORDER BY id LIMIT 1 OFFSET 4
    #   # UPDATE albums SET x = x + 1 WHERE ((name <= 'M') AND ("id" < 9) AND (id >= 5))
    #   # ...
    #   # SELECT id FROM albums WHERE ((name <= 'M') AND (id >= 12345)) ORDER BY id LIMIT 1 OFFSET 4
    #   # UPDATE albums SET x = x + 1 WHERE ((name <= 'M') AND (id >= 12345))
    # 
    # You should avoid using +paged_update+ or +paged_datasets+
    # with updates that modify the primary key, as such usage is
    # not supported by this plugin.
    #
    # This plugin only supports models with scalar primary keys.
    #
    # Usage:
    #
    #   # Make all model subclasses support paged update/delete/datasets
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :paged_operations
    #
    #   # Make the Album class support paged update/delete/datasets
    #   Album.plugin :paged_operations
    module PagedOperations
      module ClassMethods
        Plugins.def_dataset_methods(self, [:paged_datasets, :paged_delete, :paged_update])
      end

      module DatasetMethods
        # Yield datasets for subsets of the receiver that are limited
        # to no more than 1000 rows (you can configure the number of
        # rows using +:rows_per_page+).
        #
        # Options:
        # :rows_per_page :: The maximum number of rows in each yielded dataset
        #                   (unless concurrent modifications are made to the table).
        def paged_datasets(opts=OPTS)
          unless defined?(yield)
            return enum_for(:paged_datasets, opts)
          end

          pk = _paged_operations_pk(:paged_update)
          base_offset_ds = offset_ds = _paged_operations_offset_ds(opts)
          first = nil

          while last = offset_ds.get(pk)
            ds = where(pk < last)
            ds = ds.where(pk >= first) if first
            yield ds
            first = last
            offset_ds = base_offset_ds.where(pk >= first)
          end

          ds = self
          ds = ds.where(pk >= first) if first
          yield ds
          nil
        end

        # Delete all rows of the dataset using using multiple queries so that
        # no more than 1000 rows are deleted at a time (you can configure the
        # number of rows using +:rows_per_page+).
        #
        # Options:
        # :rows_per_page :: The maximum number of rows affected by each DELETE query
        #                   (unless concurrent modifications are made to the table).
        def paged_delete(opts=OPTS)
          if (db.database_type == :oracle && !supports_fetch_next_rows?) || (db.database_type == :mssql && !is_2012_or_later?)
            raise Error, "paged_delete is not supported on MSSQL/Oracle when using emulated offsets"
          end
          pk = _paged_operations_pk(:paged_delete)
          rows_deleted = 0
          offset_ds = _paged_operations_offset_ds(opts)
          while last = offset_ds.get(pk)
            rows_deleted += where(pk < last).delete
          end
          rows_deleted + delete
        end

        # Update all rows of the dataset using using multiple queries so that
        # no more than 1000 rows are updated at a time (you can configure the
        # number of rows using +:rows_per_page+). All arguments are
        # passed to Dataset#update.
        #
        # Options:
        # :rows_per_page :: The maximum number of rows affected by each UPDATE query
        #                   (unless concurrent modifications are made to the table).
        def paged_update(values, opts=OPTS)
          rows_updated = 0
          paged_datasets(opts) do |ds|
            rows_updated += ds.update(values)
          end
          rows_updated
        end

        private

        # Run some basic checks common to paged_{datasets,delete,update}
        # and return the primary key to operate on as a Sequel::Identifier.
        def _paged_operations_pk(meth)
          raise Error, "cannot use #{meth} if dataset has a limit or offset" if @opts[:limit] || @opts[:offset]
          if db.database_type == :db2 && db.offset_strategy == :emulate
            raise Error, "the paged_operations plugin is not supported on DB2 when using emulated offsets, set the :offset_strategy Database option to 'limit_offset' or 'offset_fetch'"
          end

          case pk = unambiguous_primary_key
          when Symbol
            Sequel.identifier(pk)
          when Array
            raise Error, "cannot use #{meth} on a model with a composite primary key"
          when nil
            raise Error, "cannot use #{meth} on a model without a primary key"
          else
            # Likely SQL::QualifiedIdentifier, if the dataset is joined.
            pk
          end
        end

        # The dataset that will be used by paged_{datasets,delete,update}
        # to get the upper limit for the next query.
        def _paged_operations_offset_ds(opts)
          if rows_per_page = opts[:rows_per_page]
            raise Error, ":rows_per_page option must be at least 1" unless rows_per_page >= 1
          end
          _force_primary_key_order.offset(rows_per_page || 1000)
        end
      end
    end
  end
end
