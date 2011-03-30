require 'odbc'

module Sequel
  module ODBC
    class Database < Sequel::Database
      set_adapter_scheme :odbc

      GUARDED_DRV_NAME = /^\{.+\}$/.freeze
      DRV_NAME_GUARDS = '{%s}'.freeze
      DISCONNECT_ERRORS = /\A08S01/.freeze 

      # Whether all strings should be prefixed with "N" to imply treat as
      # unicode strings. True by default, can be set to false to greatly
      # increase performance depending on your database configuration.
      attr_accessor :mssql_unicode_strings

      def initialize(opts)
        super
        case @opts[:db_type]
        when 'mssql'
          Sequel.ts_require 'adapters/odbc/mssql'
          extend Sequel::ODBC::MSSQL::DatabaseMethods
        when 'progress'
          Sequel.ts_require 'adapters/shared/progress'
          extend Sequel::Progress::DatabaseMethods
        end
        @mssql_unicode_strings = typecast_value_boolean(@opts.fetch(:mssql_unicode_strings, true))
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
            drv.attrs[param.to_s.upcase] = value.to_s
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
    
      def execute(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            r = log_yield(sql){conn.run(sql)}
            yield(r) if block_given?
          rescue ::ODBC::Error, ArgumentError => e
            raise_error(e, :disconnect=>DISCONNECT_ERRORS.match(e.message))
          ensure
            r.drop if r
          end
          nil
        end
      end
      
      def execute_dui(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.do(sql)}
          rescue ::ODBC::Error, ArgumentError => e
            raise_error(e, :disconnect=>DISCONNECT_ERRORS.match(e.message))
          end
        end
      end
      alias do execute_dui

      private
      
      def connection_execute_method
        :do
      end

      def disconnect_connection(c)
        c.disconnect
      end
    end
    
    class Dataset < Sequel::Dataset
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      ODBC_DATE_FORMAT = "{d '%Y-%m-%d'}".freeze
      TIMESTAMP_FORMAT="{ts '%Y-%m-%d %H:%M:%S'}".freeze

      # Whether all strings should be prefixed with "N" to imply treat as
      # unicode strings. True by default, can be set to false to greatly
      # increase performance depending on your database configuration.
      attr_accessor :mssql_unicode_strings

      # Use the mssql_unicode_strings default setting from the database
      def initialize(db, opts={})
        @mssql_unicode_strings = db.mssql_unicode_strings
        super
      end

      def fetch_rows(sql)
        execute(sql) do |s|
          i = -1
          cols = s.columns(true).map{|c| [output_identifier(c.name), i+=1]}
          @columns = cols.map{|c| c.at(0)}
          if rows = s.fetch_all
            rows.each do |row|
              hash = {}
              cols.each{|n,i| hash[n] = convert_odbc_value(row[i])}
              yield hash
            end
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
          Sequel.database_to_application_timestamp([v.year, v.month, v.day, v.hour, v.minute, v.second])
        when ::ODBC::Time
          now = ::Time.now
          Sequel.database_to_application_timestamp([now.year, now.month, now.day, v.hour, v.minute, v.second])
        when ::ODBC::Date
          Date.new(v.year, v.month, v.day)
        else
          v
        end
      end
      
      def default_timestamp_format
        TIMESTAMP_FORMAT
      end

      def literal_date(v)
        v.strftime(ODBC_DATE_FORMAT)
      end
      
      def literal_false
        BOOL_FALSE
      end
      
      def literal_true
        BOOL_TRUE
      end
    end
  end
end
