module Sequel
  module Fdbsql
    # A module containing the supports-style methods that exist on a database in Sequel
    # corresponds to database/features.rb in actual Sequel
    module Features

      # indexes are namespaced per table
      def global_index_namespace?
        false
      end

      # Fdbsql parser does not support deferrable constraints
      def supports_deferrable_constraints?
        false
      end

      # Fdbsql supports deferrable fk constraints
      def supports_deferrable_foreign_key_constraints?
        true
      end

      # the sql layer supports CREATE TABLE IF NOT EXISTS syntax,
      def supports_create_table_if_not_exists?
        true
      end

      # the sql layer supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
      end

      def supports_schema_parsing?
        true
      end
    end
  end
end
