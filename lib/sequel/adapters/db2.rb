require 'db2/db2cli'
Sequel.require %w'shared/db2', 'adapters'

module Sequel
  module DB2

    @convert_smallint_to_bool = true

    # Underlying error raised by Sequel, since ruby-db2 doesn't
    # use exceptions.
    class DB2Error < StandardError
    end

    class << self
      # Whether to convert smallint values to bool, true by default.
      # Can also be overridden per dataset.
      attr_accessor :convert_smallint_to_bool
    end

    tt = Class.new do
      def boolean(s) !s.to_i.zero? end
      def date(s) Date.new(s.year, s.month, s.day) end
      def time(s) Sequel::SQLTime.create(s.hour, s.minute, s.second) end
    end.new

    # Hash holding type translation methods, used by Dataset#fetch_rows.
    DB2_TYPES = {
      :boolean  => tt.method(:boolean),
      DB2CLI::SQL_BLOB => ::Sequel::SQL::Blob.method(:new),
      DB2CLI::SQL_TYPE_DATE => tt.method(:date),
      DB2CLI::SQL_TYPE_TIME => tt.method(:time),
      DB2CLI::SQL_DECIMAL => ::BigDecimal.method(:new)
    }

    class Database < Sequel::Database
      include DatabaseMethods

      set_adapter_scheme :db2

      TEMPORARY = 'GLOBAL TEMPORARY '.freeze
      _, NullHandle = DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_ENV, DB2CLI::SQL_NULL_HANDLE)
      
      # Hash of connection procs for converting
      attr_reader :conversion_procs

      def connect(server)
        opts = server_opts(server)
        dbc = checked_error("Could not allocate database connection"){DB2CLI.SQLAllocHandle(DB2CLI::SQL_HANDLE_DBC, NullHandle)}
        checked_error("Could not connect to database"){DB2CLI.SQLConnect(dbc, opts[:database], opts[:user], opts[:password])}
        dbc
      end
      
      def disconnect_connection(conn)
        DB2CLI.SQLDisconnect(conn)
        DB2CLI.SQLFreeHandle(DB2CLI::SQL_HANDLE_DBC, conn)
      end

      def execute(sql, opts=OPTS, &block)
        synchronize(opts[:server]){|conn| log_connection_execute(conn, sql, &block)}
      end

      def execute_insert(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          log_connection_execute(conn, sql)
          sql = "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1"
          log_connection_execute(conn, sql) do |sth|
            _, _, datatype, size, _, _ = checked_error("Could not describe column"){DB2CLI.SQLDescribeCol(sth, 1, 256)}
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
          e = DB2Error.new("#{ERROR_MAP[rc]}: #{msg}")
          e.set_backtrace(caller)
          raise_error(e, :disconnect=>true)
        else
          e = DB2Error.new("#{ERROR_MAP[rc] || "Error code #{rc}"}: #{msg}")
          e.set_backtrace(caller)
          raise_error(e, :disconnect=>true)
        end
      end

      def checked_error(msg)
        rc, *ary= yield
        check_error(rc, msg)
        ary.length <= 1 ? ary.first : ary
      end

      def to_application_timestamp_db2(v)
        to_application_timestamp(v.to_s)
      end

      private

      def adapter_initialize
        @conversion_procs = DB2_TYPES.dup
        @conversion_procs[DB2CLI::SQL_TYPE_TIMESTAMP] = method(:to_application_timestamp_db2)
      end

      def database_error_classes
        [DB2Error]
      end

      def begin_transaction(conn, opts=OPTS)
        log_yield(TRANSACTION_BEGIN){DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_OFF)}
        set_transaction_isolation(conn, opts)
      end

      def remove_transaction(conn, committed)
        DB2CLI.SQLSetConnectAttr(conn, DB2CLI::SQL_ATTR_AUTOCOMMIT, DB2CLI::SQL_AUTOCOMMIT_ON)
      ensure
        super
      end

      def rollback_transaction(conn, opts=OPTS)
        log_yield(TRANSACTION_ROLLBACK){DB2CLI.SQLEndTran(DB2CLI::SQL_HANDLE_DBC, conn, DB2CLI::SQL_ROLLBACK)}
      end

      def commit_transaction(conn, opts=OPTS)
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

      # Convert smallint type to boolean if convert_smallint_to_bool is true
      def schema_column_type(db_type)
        if DB2.convert_smallint_to_bool && db_type =~ /smallint/i 
          :boolean
        else
          super
        end
      end
    end
    
    class Dataset < Sequel::Dataset
      include DatasetMethods

      Database::DatasetClass = self
      MAX_COL_SIZE = 256
      
      # Whether to convert smallint to boolean arguments for this dataset.
      # Defaults to the DB2 module setting.
      def convert_smallint_to_bool
        defined?(@convert_smallint_to_bool) ? @convert_smallint_to_bool : (@convert_smallint_to_bool = DB2.convert_smallint_to_bool)
      end

      # Override the default DB2.convert_smallint_to_bool setting for this dataset.
      attr_writer :convert_smallint_to_bool

      def fetch_rows(sql)
        execute(sql) do |sth|
          db = @db
          column_info = get_column_info(sth)
          cols = column_info.map{|c| c.at(1)}
          @columns = cols
          errors = [DB2CLI::SQL_NO_DATA_FOUND, DB2CLI::SQL_ERROR]
          until errors.include?(rc = DB2CLI.SQLFetch(sth))
            db.check_error(rc, "Could not fetch row")
            row = {}
            column_info.each do |i, c, t, s, pr|
              v, _ = db.checked_error("Could not get data"){DB2CLI.SQLGetData(sth, i, t, s)}
              row[c] = if v == DB2CLI::Null
                nil
              elsif pr
                pr.call(v)
              else
                v
              end
            end
            yield row
          end
        end
        self
      end
      
      private

      def get_column_info(sth)
        db = @db
        column_count = db.checked_error("Could not get number of result columns"){DB2CLI.SQLNumResultCols(sth)}
        convert = convert_smallint_to_bool
        cps = db.conversion_procs

        (1..column_count).map do |i| 
          name, _, datatype, size, digits, _ = db.checked_error("Could not describe column"){DB2CLI.SQLDescribeCol(sth, i, MAX_COL_SIZE)}
          pr = if datatype == DB2CLI::SQL_SMALLINT && convert && size <= 5 && digits <= 1
            cps[:boolean]
          elsif datatype == DB2CLI::SQL_CLOB && Sequel::DB2.use_clob_as_blob
            cps[DB2CLI::SQL_BLOB]
          else
            cps[datatype]
          end
          [i, output_identifier(name), datatype, size, pr]
        end 
      end
    end
  end
end
