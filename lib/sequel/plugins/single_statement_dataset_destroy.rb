# frozen-string-literal: true

module Sequel
  module Plugins
    # The single_statement_dataset_destroy plugin makes the
    # model dataset.destroy method delete all rows in a
    # single DELETE statement. It runs all before_destroy
    # hooks before the DELETE, and all after_destroy hooks
    # after the delete.
    #
    # This is not compatible with around_destroy hooks,
    # so if the model is using custom around_destroy hooks,
    # dataset.destroy falls back to a separate DELETE statement
    # per row.
    #
    # Usage:
    #
    #   # Make all model subclasses use a single DELETE
    #   # statement for dataset.destroy
    #   Sequel::Model.plugin :single_statement_dataset_destroy
    #
    #   # Make the Album class use a single DELETE
    #   # statement for dataset.destroy
    #   Album.plugin :single_statement_dataset_destroy
    module SingleStatementDatasetDestroy
      module DatasetMethods
        # Destroy all rows in a single DELETE statement. Run the before_destroy
        # hooks for all rows before the DELETE, and all after_destroy hooks
        # for all rows after the DELETE. If the model uses an around_destroy
        # hook, fallback to using a separate DELETE statement per row.
        def destroy
          return super unless model.instance_method(:around_destroy).owner == Sequel::Model::InstanceMethods

          db.transaction do
            rows = all
            rows.each(&:before_destroy)
            expected_rows = rows.length
            n = delete
            unless n == expected_rows
              raise Error, "dataset changed during destroy, expected rows: #{expected_rows}, actual rows: #{n}"
            end
            rows.each(&:after_destroy)
            n
          end
        end
      end
    end
  end
end
