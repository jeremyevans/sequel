if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'db2/db2cli'

module Sequel
  module DB2
    class Database < Sequel::Database
      set_adapter_scheme :db2
      include DB2CLI
      
      # AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      # 
      # def auto_increment_sql
      #   AUTO_INCREMENT
      # end
      
      def check_error(rc, msg)
        case rc
        when SQL_SUCCESS, SQL_SUCCESS_WITH_INFO: nil
        else
          raise SequelError, msg
        end
      end
      
      rc, @@env = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE)
      check_error(rc, "Could not allocate DB2 environment")

      def connect
        rc, dbc = SQLAllocHandle(SQL_HANDLE_DBC, @@env) 
        check_error(rc, "Could not allocate database connection")
        
        rc = SQLConnect(dbc, @opts[:database], @opts[:user], @opts[:password]) 
        check_error(rc, "Could not connect to database")
        
        dbc
      end
      
      def disconnect
        @pool.disconnect do |conn|
          rc = SQLDisconnect(conn)
          check_error(rc, "Could not disconnect from database")

          rc = SQLFreeHandle(SQL_HANDLE_DBC, conn)
          check_error(rc, "Could not free Database handle")
        end
      end
    
      def test_connection
        @pool.hold {|conn|}
        true
      end

      def dataset(opts = nil)
        DB2::Dataset.new(self, opts)
      end
      
      def execute(sql, &block)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          rc, sth = SQLAllocHandle(SQL_HANDLE_STMT, @handle) 
          check_error(rc, "Could not allocate statement")

          begin
            rc = SQLExecDirect(sth, sql) 
            check_error(rc, "Could not execute statement")
            
            block[sth] if block

            rc, rpc = SQLRowCount(sth)
            check_error(rc, "Could not get RPC") 
            rpc
          ensure
            rc = SQLFreeHandle(SQL_HANDLE_STMT, sth)
            check_error(rc, "Could not free statement")
          end
        end
      end
      alias_method :do, :execute
    end
    
    class Dataset < Sequel::Dataset
      def literal(v)
        case v
        when Time: literal(v.iso8601)
        else
          super
        end
      end

      def fetch_rows(sql, &block)
        @db.synchronize do
          @db.execute(sql) do |sth|
            @column_info = get_column_info(sth)
            @columns = @column_info.map {|c| c[:name]}
            while (rc = SQLFetch(@handle)) != SQL_NO_DATA_FOUND
              @db.check_error(rc, "Could not fetch row")
              yield hash_row(sth)
            end
          end
        end
        self
      end
      
      MAX_COL_SIZE = 256

      def get_column_info(sth)
        rc, column_count = SQLNumResultCols(sth)
        @db.check_error(rc, "Could not get number of result columns")

        (1..column_count).map do |i| 
          rc, name, buflen, datatype, size, digits, nullable = SQLDescribeCol(sth, i, MAX_COL_SIZE)
          @b.check_error(rc, "Could not describe column")
          
          {:name => name, :db2_type => datatype, :precision => size}
        end 
      end
      
      def hash_row(sth)
        row = {}
        @column_info.each_with_index do |c, i|
          rc, v = SQLGetData(sth, i+1, c[:db2_type], c[:precision]) 
          @db.check_error(rc, "Could not get data")
          
          @row[c[:name]] = convert_type(v)
        end
        row
      end
      
      def convert_type(v)
        case v
        when DB2CLI::Date 
          DBI::Date.new(v.year, v.month, v.day)
        when DB2CLI::Time
          DBI::Time.new(v.hour, v.minute, v.second)
        when DB2CLI::Timestamp 
          DBI::Timestamp.new(v.year, v.month, v.day,
            v.hour, v.minute, v.second, v.fraction)
        when DB2CLI::Null
          nil
        else  
          v
        end
      end
    
      def insert(*values)
        @db.do insert_sql(*values)
      end
    
      def update(values, opts = nil)
        @db.do update_sql(values, opts)
      end
    
      def delete(opts = nil)
        @db.do delete_sql(opts)
      end
    end
  end
end