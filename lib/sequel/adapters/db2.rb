require 'db2/db2cli'
Sequel.require %w'shared/db2', 'adapters'

module Sequel
  module DB2
    class Database < Sequel::Database
      include DatabaseMethods

      set_adapter_scheme :db2

      TEMPORARY = 'GLOBAL TEMPORARY '.freeze
      rc, NullHandle = DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_ENV, DB2CLI::SQL_NULL_HANDLE)
      

      def connect(server)
        opts = server_opts(server)
        dbc = checked_error("Could not allocate database connection"){DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_DBC, NullHandle)}
        checked_error("Could not connect to database"){DB2CLI.SQLConnect(dbc, opts[:database], opts[:user], opts[:password])}
        dbc
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
            name, buflen, datatype, size, digits, nullable = checked_error("Could not describe column"){DB2CLI.SQLDescribeCol(sth, 1, 256)}
            if DB2CLI.SQLFetch(sth) != DB2CLI::SQL_NO_DATA_FOUND
              v, _ = checked_error("Could not get data"){DB2CLI.SQLGetData(sth, 1, datatype, size)}
              if v.is_a?(String) 
                return v.to_i
              else
                return nil
              end
            end
          end
        end
      end
      
      ERROR_MAP = {}
      %w'SQL_INVALID_HANDLE SQL_STILL_EXECUTING SQL_ERROR'.each do |s|
        ERROR_MAP[DB2CLI.const_get(s)] = s
      end
      def check_error(rc, msg)
        case rc
        when DB2CLI::SQL_SUCCESS, DB2CLI::SQL_SUCCESS_WITH_INFO, DB2CLI::SQL_NO_DATA_FOUND
          nil
        when DB2CLI::SQL_INVALID_HANDLE, DB2CLI::SQL_STILL_EXECUTING
          raise DatabaseDisconnectError, "#{ERROR_MAP[rc]}: #{msg}"
        else
          raise DatabaseError, "#{ERROR_MAP[rc] || "Error code #{rc}"}: #{msg}"
        end
      end

      def checked_error(msg)
        rc, *ary= yield
        check_error(rc, msg)
        ary.length <= 1 ? ary.first : ary
      end

      private

      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_OFF)}
      end

      def remove_transaction(conn, committed)
        DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_ON)
      ensure
        super
      end

      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){DB2CLI.SQLEndTran(DB2CLI::SQL_HANDLE_DBC, conn, DB2CLI::SQL_ROLLBACK)}
      end

      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){DB2CLI.SQLEndTran(DB2CLI::SQL_HANDLE_DBC, conn, DB2CLI::SQL_COMMIT)}
      end
    
      def log_connection_execute(conn, sql)
        sth = checked_error("Could not allocate statement"){DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_STMT, conn)}

        begin
          checked_error("Could not execute statement: #{sql}"){log_yield(sql){DB2CLI.SQLExecDirect(sth, sql)}}
          
          if block_given?
            yield(sth)
          else
            checked_error("Could not get RPC"){DB2CLI.SQLRowCount(sth)}
          end
        ensure
          checked_error("Could not free statement"){DB2CLI.SQLFreeHandle(DB2CLI::SQL_HANDLE_STMT, sth)}
        end
      end

      def disconnect_connection(conn)
        checked_error("Could not disconnect from database"){DB2CLI.SQLDisconnect(conn)}
        checked_error("Could not free Database handle"){DB2CLI.SQLFreeHandle(DB2CLI::SQL_HANDLE_DBC, conn)}
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
              v, _ = db.checked_error("Could not get data"){DB2CLI.SQLGetData(sth, i, t, s)}
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
        column_count = db.checked_error("Could not get number of result columns"){DB2CLI.SQLNumResultCols(sth)}

        (1..column_count).map do |i| 
          name, buflen, datatype, size, digits, nullable = db.checked_error("Could not describe column"){DB2CLI.SQLDescribeCol(sth, i, MAX_COL_SIZE)}
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
          db.to_application_timestamp(v.to_s)
        when DB2CLI::Null
          nil
        else  
          v
        end
      end
    end
  end
end
