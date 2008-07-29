require 'win32ole'

module Sequel
  # The ADO adapter provides connectivity to ADO databases in Windows. ADO
  # databases can be opened using a URL with the ado schema:
  #
  #   DB = Sequel.open('ado://mydb')
  # 
  # or using the Sequel.ado method:
  #
  #   DB = Sequel.ado('mydb')
  #
  module ADO
    class Database < Sequel::Database
      set_adapter_scheme :ado

      def connect
        @opts[:driver] ||= 'SQL Server'
        case @opts[:driver]
        when 'SQL Server'
          require 'sequel_core/adapters/shared/mssql'
          extend Sequel::MSSQL::DatabaseMethods
        end
        s = "driver=#{@opts[:driver]};server=#{@opts[:host]};database=#{@opts[:database]}#{";uid=#{@opts[:user]};pwd=#{@opts[:password]}" if @opts[:user]}"
        handle = WIN32OLE.new('ADODB.Connection')
        handle.Open(s)
        handle
      end
      
      def disconnect
        @pool.disconnect {|conn| conn.Close}
      end
    
      def dataset(opts = nil)
        ADO::Dataset.new(self, opts)
      end
    
      def execute(sql)
        log_info(sql)
        @pool.hold {|conn| conn.Execute(sql)}
      end
      alias_method :do, :execute
    end
    
    class Dataset < Sequel::Dataset
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

        if order = opts[:order]
          sql << " ORDER BY #{expression_list(order)}"
        end

        if union = opts[:union]
          sql << (opts[:union_all] ? \
              " UNION ALL #{union.sql}" : " UNION #{union.sql}")
        end
              
        raise Error, "Intersect not supported" if opts[:intersect]
        raise Error, "Except not supported" if opts[:except]

        sql
      end
      
      def literal(v)
        case v
        when Time
          literal(v.iso8601)
        when Date, DateTime
          literal(v.to_s)
        else
          super
        end
      end

      def fetch_rows(sql, &block)
        @db.synchronize do
          s = @db.execute sql
          
          @columns = s.Fields.extend(Enumerable).map {|x| x.Name.to_sym}
          
          unless s.eof
            s.moveFirst
            s.getRows.transpose.each {|r| yield hash_row(r)}
          end
        end
        self
      end
      
      def hash_row(row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
        end
      end
    end
  end
end
