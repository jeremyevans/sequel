require 'sequel_core/adapters/utils/date_format'
require 'sequel_core/adapters/utils/unsupported'

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
      include Dataset::UnsupportedIntersectExcept
      include Dataset::SQLStandardDateFormat

      SELECT_CLAUSE_ORDER = %w'limit distinct columns from join where group order having compounds'.freeze

      private

      def select_clause_order
        SELECT_CLAUSE_ORDER
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
