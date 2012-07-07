module Sequel
  module Progress
    module DatabaseMethods

      # Progress uses the :progress database type.
      def database_type
        :progress
      end
    end

    module DatasetMethods
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'select limit distinct columns from join where group order having compounds')

      # Progress requires SQL standard datetimes
      def requires_sql_standard_datetimes?
        true
      end

      # Progress does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      # Progress uses TOP for limit, but it is only supported in Progress 10.
      # The Progress adapter targets Progress 9, so it silently ignores the option.
      def select_limit_sql(sql)
        raise(Error, "OFFSET not supported") if @opts[:offset]
        # if l = @opts[:limit]
        #   sql << " TOP "
        #   literal_append(sql, l)
        # end
      end
    end
  end
end
