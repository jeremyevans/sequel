# frozen-string-literal: true

module Sequel
  module Plugins
    # This plugin implements optimistic locking mechanism on PostgreSQL based
    # on the xmin of the row. The xmin system column is automatically set to
    # the current transaction id whenever the row is inserted or updated:
    # 
    #   class Person < Sequel::Model
    #     plugin :pg_xmin_optimistic_locking
    #   end
    #   p1 = Person[1]
    #   p2 = Person[1]
    #   p1.update(name: 'Jim') # works
    #   p2.update(name: 'Bob') # raises Sequel::NoExistingObject
    #
    # The advantage of pg_xmin_optimistic_locking plugin compared to the
    # regular optimistic_locking plugin as that it does not require any
    # additional columns setup on the model.  This allows it to be loaded
    # in the base model and have all subclasses automatically use
    # optimistic locking.  The disadvantage is that testing can be
    # more difficult if you are modifying the underlying row between
    # when a model is retrieved and when it is saved.
    #
    # This plugin may not work with the class_table_inheritance plugin.
    #
    # This plugin relies on the instance_filters plugin.
    module PgXminOptimisticLocking
      WILDCARD = LiteralString.new('*').freeze
      
      # Define the xmin column accessor
      def self.apply(model)
        model.instance_exec do
          plugin(:optimistic_locking_base)
          @lock_column = :xmin
          def_column_accessor(:xmin)
        end
      end

      # Update the dataset to append the xmin column if it is usable
      # and there is a dataset for the model.
      def self.configure(model)
        model.instance_exec do
          set_dataset(@dataset) if @dataset
        end
      end

      module ClassMethods
        private

        # Ensure the dataset selects the xmin column if doing so 
        def convert_input_dataset(ds)
          append_xmin_column_if_usable(super)
        end

        # If the xmin column is not already selected, and selecting it does not
        # raise an error, append it to the selections.
        def append_xmin_column_if_usable(ds)
          select = ds.opts[:select]

          unless select && select.include?(:xmin)
            xmin_ds = ds.select_append(:xmin)
            begin
              columns = xmin_ds.columns!
            rescue Sequel::DatabaseConnectionError, Sequel::DatabaseDisconnectError
              raise
            rescue Sequel::DatabaseError
              # ignore, could be view, subquery, table returning function, etc.
            else
              ds = xmin_ds if columns.include?(:xmin)
            end
          end

          ds
        end
      end

      module InstanceMethods
        private

        # Only set the lock column instance filter if there is an xmin value. 
        def lock_column_instance_filter
          super if @values[:xmin]
        end

        # Include xmin value when inserting initial row
        def _insert_dataset
          super.returning(WILDCARD, :xmin)
        end
        
        # Remove the xmin from the columns to update.
        # PostgreSQL automatically updates the xmin value, and it cannot be assigned.
        def _save_update_all_columns_hash
          v = super
          v.delete(:xmin)
          v
        end

        # Add an RETURNING clause to fetch the updated xmin when updating the row.
        def _update_without_checking(columns)
          ds = _update_dataset
          rows = ds.clone(ds.send(:default_server_opts, :sql=>ds.returning(:xmin).update_sql(columns))).all
          values[:xmin] = rows.first[:xmin] unless rows.empty?
          rows.length
        end
      end
    end
  end
end
