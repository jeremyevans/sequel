module Sequel
  class Dataset
    # Whether this dataset quotes identifiers.
    def quote_identifiers?
      @quote_identifiers
    end
    
    # Whether the dataset requires SQL standard datetimes (false by default,
    # as most allow strings with ISO 8601 format.
    def requires_sql_standard_datetimes?
      false
    end

    # Whether the dataset supports common table expressions (the WITH clause).
    def supports_cte?
      select_clause_methods.include?(WITH_SUPPORTED)
    end

    # Whether the dataset supports the DISTINCT ON clause, false by default.
    def supports_distinct_on?
      false
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
    
    # Whether modifying joined datasets is supported.
    def supports_modifying_joins?
      false
    end
    
    # Whether the IN/NOT IN operators support multiple columns when an
    # array of values is given.
    def supports_multiple_column_in?
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
  end
end
