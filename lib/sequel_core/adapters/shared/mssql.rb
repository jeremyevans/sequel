module Sequel
  module MSSQL
    module DatabaseMethods
      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      def dataset(opts = nil)
        ds = super
        ds.extend(DatasetMethods)
        ds
      end
    end
  
    module DatasetMethods
      def complex_expression_sql(op, args)
        case op
        when :'||'
          super(:+, args)
        else
          super(op, args)
        end
      end
      
      def full_text_search(cols, terms, opts = {})
        filter("CONTAINS (#{literal(cols)}, #{literal(terms)})")
      end
      
      # Allows you to do .nolock on a query
      def nolock
        clone(:with => "(NOLOCK)")
      end

      # Formats a SELECT statement using the given options and the dataset
      # options.
      def select_sql(opts = nil)
        opts = opts ? @opts.merge(opts) : @opts

        if sql = opts[:sql]
          return sql
        end

        # ADD TOP to SELECT string for LIMITS
        if limit = opts[:limit]
          top = "TOP #{limit} "
          raise Error, "Offset not supported" if opts[:offset]
        end

        columns = opts[:select]
        select_columns = columns ? column_list(columns) : WILDCARD

        if distinct = opts[:distinct]
          distinct_clause = distinct.empty? ? "DISTINCT" : "DISTINCT ON (#{expression_list(distinct)})"
          sql = "SELECT #{top}#{distinct_clause} #{select_columns}"
        else
          sql = "SELECT #{top}#{select_columns}"
        end

        if opts[:from]
          sql << " FROM #{source_list(opts[:from])}"
        end

        # ADD WITH to SELECT string for NOLOCK
        if with = opts[:with]
          sql << " WITH #{with}"
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

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end

        if having = opts[:having]
          sql << " HAVING #{literal(having)}"
        end

        if union = opts[:union]
          sql << (opts[:union_all] ? \
            " UNION ALL #{union.sql}" : " UNION #{union.sql}")
        end
        
        raise Error, "Intersect not supported" if opts[:intersect]
        raise Error, "Except not supported" if opts[:except]
        
        sql
      end
      alias_method :sql, :select_sql
    end
  end
end
