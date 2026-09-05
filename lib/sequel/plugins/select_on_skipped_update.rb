# frozen-string-literal: true

module Sequel
  module Plugins
    # This plugin changes the default model behavior so that for existing
    # objects, if Model#save_changes skips the save as the object is not
    # modified, or if the UPDATE query is skipped as there were no columns
    # to update, Sequel will issue a SELECT query to make sure the object
    # still exists. This is only done if require_modification is set for
    # the object, so that these cases will raise the same error that
    # Sequel would raise if the UPDATE require was run but did not modify
    # a row.
    # 
    #   class Item < Sequel::Model
    #     plugin :select_on_skipped_update
    #   end
    #
    #   item = Item[1]
    #
    #   # Delete the item from the database
    #   Item.where(id: 1).delete
    #
    #   # By default, this would return nil, but with the plugin,
    #   # this raises Sequel::NoExistingRow.
    #   item.save_changes
    #
    #   # By default, this would return the object without issuing the
    #   # UPDATE query (as the columns to update is empty. With the plugin,
    #   # this raises Sequel::NoExistingRow.
    #   item.save(columns: [])
    module SelectOnSkippedUpdate
      COUNT = Sequel.function(:count).*.as(:count)
      private_constant :COUNT

      module InstanceMethods
        # If an UPDATE statement was skipped as the object wasn't modified, run a SELECT query.
        def save_changes(opts=OPTS)
          ret = super
          run_select_on_skipped_update if ret.nil? && require_modification
          ret
        end
        
        private

        # If an UPDATE statement was skipped as there were no columns to update, run a SELECT query.
        def _update_columns(columns)
          ret = super
          run_select_on_skipped_update if ret.nil? && columns.empty? && require_modification
          ret
        end

        # Run a SELECT query on the dataset to determine whether the object still exists in the database.
        # This will result in an exception if a single row is not modified.
        def run_select_on_skipped_update
          ds = _update_dataset
          unless ds.select(COUNT).single_value! == 1
            raise(NoMatchingRow, "Attempt to select object did not result in a single row modification")
          end
        end
      end
    end
  end
end
