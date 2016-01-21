# frozen-string-literal: true

module Sequel
  class Dataset
    # ---------------------
    # :section: 4 - Methods that describe what the dataset supports
    # These methods all return booleans, with most describing whether or not the
    # dataset supports a feature.
    # ---------------------
    
    # Whether this dataset quotes identifiers.
    def quote_identifiers?
      if defined?(@quote_identifiers)
        @quote_identifiers
      else
        @quote_identifiers = db.quote_identifiers?
      end
    end
    
    # Whether this dataset will provide accurate number of rows matched for
    # delete and update statements.  Accurate in this case is the number of
    # rows matched by the dataset's filter.
    def provides_accurate_rows_matched?
      true
    end
    
    # Whether you must use a column alias list for recursive CTEs (false by
    # default).
    def recursive_cte_requires_column_aliases?
      false
    end

    # Whether the dataset requires SQL standard datetimes (false by default,
    # as most allow strings with ISO 8601 format).
    def requires_sql_standard_datetimes?
      false
    end

    # Whether type specifiers are required for prepared statement/bound
    # variable argument placeholders (i.e. :bv__integer)
    def requires_placeholder_type_specifiers?
      false
    end

    # Whether the dataset supports common table expressions (the WITH clause).
    # If given, +type+ can be :select, :insert, :update, or :delete, in which case it
    # determines whether WITH is supported for the respective statement type.
    def supports_cte?(type=:select)
      false
    end

    # Whether the dataset supports common table expressions (the WITH clause)
    # in subqueries.  If false, applies the WITH clause to the main query, which can cause issues
    # if multiple WITH clauses use the same name.
    def supports_cte_in_subqueries?
      false
    end

    # Whether the database supports derived column lists (e.g.
    # "table_expr AS table_alias(column_alias1, column_alias2, ...)"), true by
    # default.
    def supports_derived_column_lists?
      true
    end

    # Whether the dataset supports or can emulate the DISTINCT ON clause, false by default.
    def supports_distinct_on?
      false
    end

    # Whether the dataset supports CUBE with GROUP BY.
    def supports_group_cube?
      false
    end

    # Whether the dataset supports ROLLUP with GROUP BY.
    def supports_group_rollup?
      false
    end

    # Whether the dataset supports GROUPING SETS with GROUP BY.
    def supports_grouping_sets?
      false
    end

    # Whether this dataset supports the +insert_select+ method for returning all columns values
    # directly from an insert query.
    def supports_insert_select?
      supports_returning?(:insert)
    end

    # Whether the dataset supports the INTERSECT and EXCEPT compound operations, true by default.
    def supports_intersect_except?
      true
    end

    # Whether the dataset supports the INTERSECT ALL and EXCEPT ALL compound operations, true by default.
    def supports_intersect_except_all?
      true
    end

    # Whether the dataset supports the IS TRUE syntax.
    def supports_is_true?
      true
    end
    
    # Whether the dataset supports the JOIN table USING (column1, ...) syntax.
    def supports_join_using?
      true
    end
    
    # Whether the dataset supports LATERAL for subqueries in the FROM or JOIN clauses.
    def supports_lateral_subqueries?
      false
    end

    # Whether limits are supported in correlated subqueries.  True by default.
    def supports_limits_in_correlated_subqueries?
      true
    end
    
    # Whether modifying joined datasets is supported.
    def supports_modifying_joins?
      false
    end
    
    # Whether the IN/NOT IN operators support multiple columns when an
    # array of values is given.
    def supports_multiple_column_in?
      true
    end

    # Whether offsets are supported in correlated subqueries, true by default.
    def supports_offsets_in_correlated_subqueries?
      true
    end

    # Whether the dataset supports or can fully emulate the DISTINCT ON clause,
    # including respecting the ORDER BY clause, false by default
    def supports_ordered_distinct_on?
      supports_distinct_on?
    end
    
    # Whether the dataset supports pattern matching by regular expressions.
    def supports_regexp?
      false
    end

    # Whether the dataset supports REPLACE syntax, false by default.
    def supports_replace?
      false
    end

    # Whether the RETURNING clause is supported for the given type of query.
    # +type+ can be :insert, :update, or :delete.
    def supports_returning?(type)
      false
    end

    # Whether the database supports SELECT *, column FROM table
    def supports_select_all_and_column?
      true
    end

    # Whether the dataset supports timezones in literal timestamps
    def supports_timestamp_timezones?
      false
    end
    
    # Whether the dataset supports fractional seconds in literal timestamps
    def supports_timestamp_usecs?
      true
    end
    
    # Whether the dataset supports window functions.
    def supports_window_functions?
      false
    end
    
    # Whether the dataset supports WHERE TRUE (or WHERE 1 for databases that
    # that use 1 for true).
    def supports_where_true?
      true
    end

    private

    # Whether insert(nil) or insert({}) must be emulated by
    # using at least one value, false by default.
    def insert_supports_empty_values?
      true
    end

    # Whether the database supports quoting function names, false by default.
    def supports_quoted_function_names?
      false
    end

    # Whether the RETURNING clause is used for the given dataset.
    # +type+ can be :insert, :update, or :delete.
    def uses_returning?(type)
      opts[:returning] && !@opts[:sql] && supports_returning?(type)
    end
    
    # Whether the dataset uses WITH ROLLUP/CUBE instead of ROLLUP()/CUBE().
    def uses_with_rollup?
      false
    end
  end
end
