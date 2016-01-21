# frozen-string-literal: true

module Sequel
  module Plugins
    # The before_after_save plugin reorders some internal
    # Sequel operations so they happen before after_create,
    # after_update, and after_save are called, instead of
    # after.  These operations are:
    # 
    # * Resetting the explicit modified flag
    # * Refreshing the model or clearing changed columns after creation
    #
    # This behavior will become the default in Sequel 5.
    #
    # Usage:
    #
    #   # Make all model subclasses perform the operations before after_save
    #   Sequel::Model.plugin :before_after_save
    #
    #   # Make the Album class perform the operations before after_save
    #   Album.plugin :before_after_save
    module BeforeAfterSave
      module InstanceMethods
        private

        # Refresh and reset modified flag right after INSERT query.
        def _after_create(pk)
          super
          @modified = false
          pk ? _save_refresh : changed_columns.clear
        end

        # Don't refresh or reset modified flag, as it was already done.
        def _after_save(pk)
          if @was_new
            @was_new = nil
          else
            @columns_updated = nil
          end
        end

        # Refresh and reset modified flag right after UPDATE query.
        def _after_update
          super
          @modified = false
        end
      end
    end
  end
end
