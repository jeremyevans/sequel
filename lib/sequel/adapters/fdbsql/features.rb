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

      # FDBSQL does not: supports_group_cube? (aligns with super)

      # FDBSQL does support GROUP BY ROLLUP
      def supports_group_rollup?
        true
      end

      # FDBSQL does: supports_intersect_except? (INTERSECT and EXCEPT) (aligns with super)

      # FDBSQL does: supports_intersect_except_all? (INTERSECT ALL and EXCEPT ALL) (aligns with super)

      # FDBSQL does: supports_is_true? (aligns with super)

      # FDBSQL does: supports_join_using? (aligns with super)

      # FDBSQL does not: supports_lateral_subqueries? (aligns with super)

      # FDBSQL does: supports_limits_in_correlated_subqueries? (aligns with super)

      # FDBSQL does not support UPDATE or DELETE on joined tables: supports_modifying_joins? (aligns with super)

      # FDBSQL does: supports_multiple_column_in? (aligns with super)

      # FDBSQL does: supports_offsets_in_correlated_subqueries? (aligns with super)

      # FDBSQL does: supports_regexp? (but with functions)
      def supports_regexp?
        true
      end

      # FDBSQL does not: supports_replace?

      # Returning is always supported.
      def supports_returning?(type)
        true
      end

      # FDBSQL does: supports_select_all_and_column? (aligns with super)

      # FDBQSL does not: supports_timestamp_timezones? (aligns with super)

      # FDBSQL truncates all seconds
      def supports_timestamp_usecs?
        false
      end

      # FDBSQL does not: supports_window_functions? (aligns with super)

      # FDBSQL does: supports_where_true? (aligns with super)

      # FDBSQL does: insert_supports_empty_values? (aligns with super)
      # i.e. INSERT DEFAULT VALUES

      # FDBSQL does not: supports_quoted_function_names? (aligns with super)

      # FDBSQL does not: uses_with_rollup? (aligns with super)
      # FDBSQL does not use WITH ROLLUP/CUBE instead of ROLLUP()/CUBE().

    end
  end
end
