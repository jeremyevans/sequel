require 'odbc'

module Sequel
  module ODBC
    class Database < Sequel::Database
      set_adapter_scheme :odbc

      GUARDED_DRV_NAME = /^\{.+\}$/.freeze
      DRV_NAME_GUARDS = '{%s}'.freeze

      def initialize(opts)
        super(opts)
        case opts[:db_type]
        when 'mssql'
          Sequel.require 'adapters/shared/mssql'
          extend Sequel::MSSQL::DatabaseMethods
        when 'progress'
          Sequel.require 'adapters/shared/progress'
          extend Sequel::Progress::DatabaseMethods
        end
      end

      def connect(server)
        opts = server_opts(server)
        if opts.include? :driver
          drv = ::ODBC::Driver.new
          drv.name = 'Sequel ODBC Driver130'
          opts.each do |param, value|
            if :driver == param and not (value =~ GUARDED_DRV_NAME)
              value = DRV_NAME_GUARDS % value
            end
            drv.attrs[param.to_s.capitalize] = value
          end
          db = ::ODBC::Database.new
          conn = db.drvconnect(drv)
        else
          conn = ::ODBC::connect(opts[:database], opts[:user], opts[:password])
        end
        conn.autocommit = true
        conn
      end      

      def dataset(opts = nil)
        ODBC::Dataset.new(self, opts)
      end
    
      # ODBC returns native statement objects, which must be dropped if
      # you call execute manually, or you will get warnings.  See the
      # fetch_rows method source code for an example of how to drop
      # the statements.
      def execute(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]) do |conn|
          r = conn.run(sql)
          yield(r) if block_given?
          r
        end
      end
      
      def execute_dui(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]){|conn| conn.do(sql)}
      end
      alias_method :do, :execute_dui

      # Support single level transactions on ODBC
      def transaction(opts={})
        unless opts.is_a?(Hash)
          Deprecation.deprecate('Passing an argument other than a Hash to Database#transaction', "Use DB.transaction(:server=>#{opts.inspect})") 
          opts = {:server=>opts}
        end
        synchronize(opts[:server]) do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
          log_info(begin_transaction_sql)
          conn.do(begin_transaction_sql)
          begin
            @transactions << Thread.current
            yield(conn)
          rescue ::Exception => e
            log_info(rollback_transaction_sql)
            conn.do(rollback_transaction_sql)
            transaction_error(e)
          ensure
            unless e
              log_info(commit_transaction_sql)
              conn.do(commit_transaction_sql)
            end
            @transactions.delete(Thread.current)
          end
        end
      end

      private

      def disconnect_connection(c)
        c.disconnect
      end
    end
    
    class Dataset < Sequel::Dataset
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      ODBC_TIMESTAMP_FORMAT = "{ts '%Y-%m-%d %H:%M:%S'}".freeze
      ODBC_TIMESTAMP_AFTER_SECONDS =
        ODBC_TIMESTAMP_FORMAT.index( '%S' ).succ - ODBC_TIMESTAMP_FORMAT.length
      ODBC_DATE_FORMAT = "{d '%Y-%m-%d'}".freeze
      UNTITLED_COLUMN = 'untitled_%d'.freeze

      def fetch_rows(sql, &block)
        execute(sql) do |s|
          begin
            untitled_count = 0
            @columns = s.columns(true).map do |c|
              if (n = c.name).empty?
                n = UNTITLED_COLUMN % (untitled_count += 1)
              end
              output_identifier(n)
            end
            rows = s.fetch_all
            rows.each {|row| yield hash_row(row)} if rows
          ensure
            s.drop unless s.nil? rescue nil
          end
        end
        self
      end
      
      private

      def convert_odbc_value(v)
        # When fetching a result set, the Ruby ODBC driver converts all ODBC 
        # SQL types to an equivalent Ruby type; with the exception of
        # SQL_TYPE_DATE, SQL_TYPE_TIME and SQL_TYPE_TIMESTAMP.
        #
        # The conversions below are consistent with the mappings in
        # ODBCColumn#mapSqlTypeToGenericType and Column#klass.
        case v
        when ::ODBC::TimeStamp
          DateTime.new(v.year, v.month, v.day, v.hour, v.minute, v.second)
        when ::ODBC::Time
          now = DateTime.now
          Time.gm(now.year, now.month, now.day, v.hour, v.minute, v.second)
        when ::ODBC::Date
          Date.new(v.year, v.month, v.day)
        else
          v
        end
      end
      
      def hash_row(row)
        hash = {}
        row.each_with_index do |v, idx|
          hash[@columns[idx]] = convert_odbc_value(v)
        end
        hash
      end
      
      def literal_date(v)
        v.strftime(ODBC_DATE_FORMAT)
      end
      
      def literal_datetime(v)
        formatted = v.strftime(ODBC_TIMESTAMP_FORMAT)
        usec = v.sec_fraction * 86400000000
        formatted.insert(ODBC_TIMESTAMP_AFTER_SECONDS, ".#{(usec.to_f/1000).round}") if usec >= 1000
        formatted
      end
      
      def literal_false
        BOOL_FALSE
      end
      
      def literal_true
        BOOL_TRUE
      end

      def literal_time(v)
        formatted = v.strftime(ODBC_TIMESTAMP_FORMAT)
        formatted.insert(ODBC_TIMESTAMP_AFTER_SECONDS, ".#{(v.usec.to_f/1000).round}") if usec >= 1000
        formatted
      end
    end
  end
end
