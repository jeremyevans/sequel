module Sequel
  module Oracle
    module DatabaseMethods
      def tables
        from(:tab).select(:tname).filter(:tabtype => 'TABLE').map do |r|
          r[:tname].downcase.to_sym
        end
      end

      def table_exists?(name)
        from(:tab).filter(:tname => name.to_s.upcase, :tabtype => 'TABLE').count > 0
      end
    end
    
    module DatasetMethods
      include Dataset::UnsupportedIntersectExceptAll

      SELECT_CLAUSE_ORDER = %w'distinct columns from join where group having union intersect except order limit'.freeze

      def empty?
        db[:dual].where(exists).get(1) == nil
      end

      private

      # Oracle doesn't support the use of AS when aliasing a dataset.  It doesn't require
      # the use of AS anywhere, so this disables it in all cases.
      def as_sql(expression, aliaz)
        "#{expression} #{quote_identifier(aliaz)}"
      end

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # Oracle doesn't support DISTINCT ON
      def select_distinct_sql(sql, opts)
        if opts[:distinct]
          raise(Error, "DISTINCT ON not supported by Oracle") unless opts[:distinct].empty?
          sql << " DISTINCT"
        end
      end

      # Oracle uses MINUS instead of EXCEPT, and doesn't support EXCEPT ALL
      def select_except_sql(sql, opts)
        sql << " MINUS #{opts[:except].sql}" if opts[:except]
      end

      # Oracle requires a subselect to do limit and offset
      def select_limit_sql(sql, opts)
        if limit = opts[:limit]
          if (offset = opts[:offset]) && (offset > 0)
            sql.replace("SELECT * FROM (SELECT raw_sql_.*, ROWNUM raw_rnum_ FROM(#{sql}) raw_sql_ WHERE ROWNUM <= #{limit + offset}) WHERE raw_rnum_ > #{offset}")
          else
            sql.replace("SELECT * FROM (#{sql}) WHERE ROWNUM <= #{limit}")
          end
        end
      end
    end
  end
end
