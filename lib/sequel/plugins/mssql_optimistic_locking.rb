# frozen-string-literal: true

module Sequel
  module Plugins
    # This plugin implements optimistic locking mechanism on Microsoft SQL Server
    # using a timestamp/rowversion column to ensure that concurrent updates are
    # detected and previous changes are not automatically overridden. This is
    # best implemented by a code example:
    # 
    #   class Person < Sequel::Model
    #     plugin :mssql_optimistic_locking
    #   end
    #   p1 = Person[1]
    #   p2 = Person[1]
    #   p1.update(name: 'Jim') # works
    #   p2.update(name: 'Bob') # raises Sequel::NoExistingObject
    #
    # In order for this plugin to work, you need to make sure that the database
    # table has a column of timestamp or rowversion.  The plugin uses a default
    # name of timestamp for this columns, but you can override that using the
    # :lock_column option:
    #
    #     plugin :mssql_optimistic_locking, lock_column: :column_name
    #
    # This plugin relies on the instance_filters plugin.
    module MssqlOptimisticLocking
      # Load the instance_filters plugin into the model.
      def self.apply(model, opts=OPTS)
        model.plugin(:optimistic_locking_base)
      end

      # Set the lock column
      def self.configure(model, opts=OPTS)
        model.lock_column = opts[:lock_column] || model.lock_column || :timestamp
      end
      
      module InstanceMethods
        private
        
        # Make the instance filter value a blob.
        def lock_column_instance_filter_value
          Sequel.blob(super)
        end

        # Remove the lock column from the columns to update.
        # SQL Server automatically updates the lock column value, and does not like
        # it to be assigned.
        def _save_update_all_columns_hash
          v = super
          v.delete(model.lock_column)
          v
        end

        # Add an OUTPUT clause to fetch the updated timestamp when updating the row.
        def _update_without_checking(columns)
          ds = _update_dataset
          lc = model.lock_column
          rows = ds.clone(ds.send(:default_server_opts, :sql=>ds.output(nil, [Sequel[:inserted][lc]]).update_sql(columns))).all
          values[lc] = rows.first[lc] unless rows.empty?
          rows.length
        end
      end
    end
  end
end
