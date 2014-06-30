module Sequel
  module Fdbsql
    # A module containing the supports-style methods that exist on a database in Sequel
    # corresponds to database/features.rb in actual Sequel
    # Methods where we maintain the same behavior as super, are replaced with comments
    # Methods that are defined based on whether we have a method defined are not
    # listed here
    module DatabaseFeatures

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

      # FDBSQL does not: supports_partial_indexes? (aligns with super)

      # TODO (as part of prepared statements) fsupports_prepared_transactions?

      # FDBSQL does not: supports_savepoints? (aligns with super)

      # FBBSQL does not: supports_transaction_isolation_levels? (aligns with super)
      # isolation levels don't apply to an MVCC sql

      # FDBSQL does not: supports_transactional_ddl? (aligns with super)

      # FDBSQL does not: supports_combining_alter_table_ops? (aligns with super)

      # FDBSQL does not: supports_create_or_replace_view? (aligns with super)

      # FDBSQL does: supports_named_column_constraints? (aligns with super)

      # FDBSQL does not support:view_with_check_option_support (aligns with super)

    end

    # A module containing the supports-style methods that exist on a dataset in Sequel
    # corresponds to dataset/features.rb in actual Sequel
    # Methods where we maintain the same behavior as super, are replaced with comments
    # Methods that are defined based on whether we have a method defined are not
    # listed here
    module DatasetFeatures

      # FDBSQL allows you to quote_identifiers? (aligns with super)

      # FDBSQL provides_accurate_rows_matched? (aligns with super)

      # FDBSQL does not support common table expressions: recursive_cte_requires_column_aliases? (aligns with super)

      # FDBSQL does not: requires_sql_standard_datetimes? (aligns with super)

      # FDBSQL does not: requires_placeholder_type_specifiers? (aligns with super)

      # FDBSQL does not support common table expressions: supports_cte?(type=:select) (aligns with super)

      # FDBSQL does not support common table expressions: supports_cte_in_subqueries? (aligns with super)

      # FDBSQL does: supports_derived_column_lists? (aligns with super)

      # FDBSQL does not: supports_distinct_on? (aligns with super)

    end
  end
end
