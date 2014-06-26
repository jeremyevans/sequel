module Sequel
  module Fdbsql
    # A module containing the supports-style methods that exist on a database in Sequel
    # corresponds to database/features.rb in actual Sequel
    module Features

      # indexes are namespaced per table
      def global_index_namespace?
        false
      end

      # Fdbsql parser does not supports_deferrable_constraints? (aligns with super)

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

      # TODO supports_foreign_key_parsing?

      # TODO supports_index_parsing?

      # FDBSQL does not: supports_partial_indexes? (aligns with super)

      def supports_schema_parsing?
        true
      end

    end
  end
end
