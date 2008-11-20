module Sequel
  module Progress
    module DatabaseMethods

      def dataset(opts = nil)
        ds = super
        ds.extend(DatasetMethods)
        ds
      end
    end
  
    module DatasetMethods
      SELECT_CLAUSE_ORDER = %w'intersect except limit distinct columns from join where group order having union'.freeze

      private

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # EXCEPT is not supported by Progress
      def select_except_sql(sql, opts)
        raise(Error, "EXCEPT not supported") if opts[:except]
      end

      # INTERSECT is not supported by Progress
      def select_intersect_sql(sql, opts)
        raise(Error, "INTERSECT not supported") if opts[:intersect]
      end

      # Progress uses TOP for limit, but it is only supported in Progress 10.
      # The Progress adapter targets Progress 9, so it silently ignores the option.
      def select_limit_sql(sql, opts)
        raise(Error, "OFFSET not supported") if opts[:offset]
        #sql << " TOP #{opts[:limit]}" if opts[:limit]
      end
    end
  end
end
