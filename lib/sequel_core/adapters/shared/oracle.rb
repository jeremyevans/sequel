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
      def empty?
        db[:dual].where(exists).get(1) == nil
      end

      # Formats a SELECT statement using the given options and the dataset
      # options.
      def select_sql(opts = nil)
        opts = opts ? @opts.merge(opts) : @opts

        if sql = opts[:sql]
          return sql
        end

        columns = opts[:select]
        select_columns = columns ? column_list(columns) : '*'
        sql = opts[:distinct] ? \
        "SELECT DISTINCT #{select_columns}" : \
        "SELECT #{select_columns}"
        
        if opts[:from]
          sql << " FROM #{source_list(opts[:from])}"
        end
        
        if join = opts[:join]
          join.each{|j| sql << literal(j)}
        end

        if where = opts[:where]
          sql << " WHERE #{literal(where)}"
        end

        if group = opts[:group]
          sql << " GROUP BY #{expression_list(group)}"
        end

        if having = opts[:having]
          sql << " HAVING #{literal(having)}"
        end

        if union = opts[:union]
          sql << (opts[:union_all] ? \
            " UNION ALL #{union.sql}" : " UNION #{union.sql}")
        elsif intersect = opts[:intersect]
          sql << (opts[:intersect_all] ? \
            " INTERSECT ALL #{intersect.sql}" : " INTERSECT #{intersect.sql}")
        elsif except = opts[:except]
          sql << (opts[:except_all] ? \
            " EXCEPT ALL #{except.sql}" : " EXCEPT #{except.sql}")
        end

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end

        if limit = opts[:limit]
          if (offset = opts[:offset]) && (offset > 0)
            sql = "SELECT * FROM (SELECT raw_sql_.*, ROWNUM raw_rnum_ FROM(#{sql}) raw_sql_ WHERE ROWNUM <= #{limit + offset}) WHERE raw_rnum_ > #{offset}"
          else
            sql = "SELECT * FROM (#{sql}) WHERE ROWNUM <= #{limit}"
          end
        end

        sql
      end
    end
  end
end
