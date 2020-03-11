# frozen-string-literal: true

module Sequel
  module Plugins
    module SkipSavingGeneratedColumns
      # The skip_saving_generated_columms plugin ensures that generated columns
      # don't get added to the list of columns which are saved to the database.
      # Generated columns are controlled by the database and therefore should
      # not be set manually.
      #
      # Usage:
      #
      #   Sequel::Model.plugin :skip_saving_generated_columns
      #
      module InstanceMethods
        private

        # Remove any columns which have a :generated property set to true
        # from the update columns hash since those columns are updated
        # by the database and should not be updated from Sequel.
        def _save_update_all_columns_hash
          super.reject { |column, _| db_schema[column][:generated] }
        end
      end
    end
  end
end
