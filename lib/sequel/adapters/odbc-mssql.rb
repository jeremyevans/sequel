if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../../sequel')
end

if !Sequel.const_defined?('ODBC')
  require File.join(File.dirname(__FILE__), 'odbc')
end

module Sequel
  module ODBC
    module MSSQL
      class Database < ODBC::Database
        set_adapter_scheme :"odbc-mssql"
        
        def dataset(opts = nil)
          MSSQL::Dataset.new(self, opts)
        end
      end
      
      class Dataset < ODBC::Dataset
        # Allows you to do .nolock on a query
        def nolock
          clone_merge(:with => "(NOLOCK)")
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
            top = "TOP #{limit}"
            raise SequelError, "Offset not supported" if opts[:offset]
          end

          columns = opts[:select]
          select_columns = columns ? column_list(columns) : WILDCARD

          if distinct = opts[:distinct]
            distinct_clause = distinct.empty? ? "DISTINCT" : "DISTINCT ON (#{column_list(distinct)})"
            sql = "SELECT #{top} #{distinct_clause} #{select_columns}"
          else
            sql = "SELECT #{top} #{select_columns}"
          end

          # ADD WITH to SELECT string for NOLOCK
          if with = opts[:with]
            sql << " WITH #{with}"
          end

          if opts[:from]
            sql << " FROM #{source_list(opts[:from])}"
          end

          if join = opts[:join]
            sql << join
          end

          if where = opts[:where]
            sql << " WHERE #{where}"
          end

          if group = opts[:group]
            sql << " GROUP BY #{column_list(group)}"
          end

          if order = opts[:order]
            sql << " ORDER BY #{column_list(order)}"
          end

          if having = opts[:having]
            sql << " HAVING #{having}"
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

          sql
        end
        alias_method :sql, :select_sql
      end
    end
  end
end