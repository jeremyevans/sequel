require 'db2/db2cli'
Sequel.require %w'shared/db2', 'adapters'

module Sequel
  module DB2
    class Database < Sequel::Database
      include DatabaseMethods

      set_adapter_scheme :db2

      TEMPORARY = 'GLOBAL TEMPORARY '.freeze
      
      rc, @@env = DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_ENV, DB2CLI::SQL_NULL_HANDLE)
      #check_error(rc, "Could not allocate DB2 environment")

      def connect(server)
        opts = server_opts(server)
        rc, dbc = DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_DBC, @@env) 
        check_error(rc, "Could not allocate database connection")
        
        rc = DB2CLI.SQLConnect(dbc, opts[:database], opts[:user], opts[:password]) 
        check_error(rc, "Could not connect to database")
        
        dbc
      end
      
      def test_connection(server=nil)
        synchronize(server){|conn|}
        true
      end

      def dataset(opts = nil)
        DB2::Dataset.new(self, opts)
      end
      
      def execute(sql, opts={}, &block)
        synchronize(opts[:server]){|conn| log_connection_execute(conn, sql, &block)}
      end
      alias do execute

      def execute_insert(sql, opts={})
        synchronize(opts[:server]) do |conn|
          log_connection_execute(conn, sql)
          sql = "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1"
          log_connection_execute(conn, sql) do |sth|
            rc, name, buflen, datatype, size, digits, nullable = DB2CLI.SQLDescribeCol(sth, 1, 256)
            check_error(rc, "Could not describe column")
            if (rc = DB2CLI.SQLFetch(sth)) != DB2CLI::SQL_NO_DATA_FOUND
              rc, v = DB2CLI.SQLGetData(sth, 1, datatype, size)
              check_error(rc, "Could not get data")
              if v.is_a?(String) 
                return v.to_i
              else
                return nil
              end
            end
          end
        end
      end
      
      def check_error(rc, msg)
        case rc
        when DB2CLI::SQL_SUCCESS, DB2CLI::SQL_SUCCESS_WITH_INFO
          nil
        else
          raise DatabaseError, msg
        end
      end

      private

      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_OFF)}
        conn
      end

      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){DB2CLI.SQLEndTran(DB2CLI::SQL_HANDLE_DBC, conn, DB2CLI::SQL_ROLLBACK)}
      ensure
        DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_ON)
      end

      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){DB2CLI.SQLEndTran(DB2CLI::SQL_HANDLE_DBC, conn, DB2CLI::SQL_COMMIT)}
      ensure
        DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_ON)
      end
    
      def log_connection_execute(conn, sql)
        rc, sth = DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_STMT, conn) 
        check_error(rc, "Could not allocate statement")

        begin
          rc = log_yield(sql){DB2CLI.SQLExecDirect(sth, sql)}
          check_error(rc, "Could not execute statement: #{sql}")
          
          yield(sth) if block_given?

          rc, rpc = DB2CLI.SQLRowCount(sth)
          check_error(rc, "Could not get RPC") 
          rpc
        ensure
          rc = DB2CLI.SQLFreeHandle(DB2CLI::SQL_HANDLE_STMT, sth)
          check_error(rc, "Could not free statement")
        end
      end

      def disconnect_connection(conn)
        rc = DB2CLI.SQLDisconnect(conn)
        check_error(rc, "Could not disconnect from database")

        rc = DB2CLI.SQLFreeHandle(DB2CLI::SQL_HANDLE_DBC, conn)
        check_error(rc, "Could not free Database handle")
      end
    end
    
    class Dataset < Sequel::Dataset
      include DatasetMethods

      MAX_COL_SIZE = 256
      
      def fetch_rows(sql)
        execute(sql) do |sth|
          offset = @opts[:offset]
          db = @db
          i = 1
          column_info = get_column_info(sth)
          cols = column_info.map{|c| c.at(1)}
          cols.delete(row_number_column) if offset
          @columns = cols
          while (rc = DB2CLI.SQLFetch(sth)) != DB2CLI::SQL_NO_DATA_FOUND
            db.check_error(rc, "Could not fetch row")
            row = {}
            column_info.each do |i, c, t, s|
              rc, v = DB2CLI.SQLGetData(sth, i, t, s) 
              db.check_error(rc, "Could not get data")
              row[c] = convert_type(v)
            end
            row.delete(row_number_column) if offset
            yield row
          end
        end
        self
      end
      
      private

      def get_column_info(sth)
        db = @db
        rc, column_count = DB2CLI.SQLNumResultCols(sth)
        db.check_error(rc, "Could not get number of result columns")

        (1..column_count).map do |i| 
          rc, name, buflen, datatype, size, digits, nullable = DB2CLI.SQLDescribeCol(sth, i, MAX_COL_SIZE)
          db.check_error(rc, "Could not describe column")
          [i, output_identifier(name), datatype, size]
        end 
      end
      
      def convert_type(v)
        case v
        when DB2CLI::Date 
          Date.new(v.year, v.month, v.day)
        when DB2CLI::Time
          Sequel::SQLTime.create(v.hour, v.minute, v.second)
        when DB2CLI::Timestamp 
          Sequel.database_to_application_timestamp(v.to_s)
        when DB2CLI::Null
          nil
        else  
          v
        end
      end
    end
  end
end
