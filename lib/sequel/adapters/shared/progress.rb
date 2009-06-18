Sequel.require %w'date_format unsupported', 'adapters/utils'

module Sequel
  module Progress
    module DatabaseMethods

      # Progress uses the :progress database type.
      def database_type
        :progress
      end

      def dataset(opts = nil)
        ds = super
        ds.extend(DatasetMethods)
        ds
      end
    end
  
    module DatasetMethods
      include Dataset::SQLStandardDateFormat

      SELECT_CLAUSE_ORDER = %w'limit distinct columns from join where group order having compounds'.freeze

      # Progress does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # Progress uses TOP for limit, but it is only supported in Progress 10.
      # The Progress adapter targets Progress 9, so it silently ignores the option.
      def select_limit_sql(sql)
        raise(Error, "OFFSET not supported") if @opts[:offset]
        #sql << " TOP #{@opts[:limit]}" if @opts[:limit]
      end
    end
  end
end
