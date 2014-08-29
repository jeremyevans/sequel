require 'odbc'

module Sequel
  module ODBC
    # Contains procs keyed on subadapter type that extend the
    # given database object so it supports the correct database type.
    DATABASE_SETUP = {}
      
    class Database < Sequel::Database
      set_adapter_scheme :odbc

      GUARDED_DRV_NAME = /^\{.+\}$/.freeze
      DRV_NAME_GUARDS = '{%s}'.freeze
      DISCONNECT_ERRORS = /\A08S01/.freeze 

      def connect(server)
        opts = server_opts(server)
        conn = if opts.include?(:drvconnect)
          ::ODBC::Database.new.drvconnect(opts[:drvconnect])
        elsif opts.include?(:driver)
          drv = ::ODBC::Driver.new
          drv.name = 'Sequel ODBC Driver130'
          opts.each do |param, value|
            if :driver == param and not (value =~ GUARDED_DRV_NAME)
              value = DRV_NAME_GUARDS % value
            end
            drv.attrs[param.to_s.upcase] = value.to_s
          end
          ::ODBC::Database.new.drvconnect(drv)
        else
          ::ODBC::connect(opts[:database], opts[:user], opts[:password])
        end
        conn.autocommit = true
        conn
      end      

      def disconnect_connection(c)
        c.disconnect
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            r = log_yield(sql){conn.run(sql)}
            yield(r) if block_given?
          rescue ::ODBC::Error, ArgumentError => e
            raise_error(e)
          ensure
            r.drop if r
          end
          nil
        end
      end
      
      def execute_dui(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.do(sql)}
          rescue ::ODBC::Error, ArgumentError => e
            raise_error(e)
          end
        end
      end

      private
      
      def adapter_initialize
        if (db_type = @opts[:db_type]) && (prok = Sequel::Database.load_adapter(db_type.to_sym, :map=>DATABASE_SETUP, :subdir=>'odbc'))
          prok.call(self)
        end
      end

      def connection_execute_method
        :do
      end

      def database_error_classes
        [::ODBC::Error]
      end

      def disconnect_error?(e, opts)
        super || (e.is_a?(::ODBC::Error) && DISCONNECT_ERRORS.match(e.message))
      end
    end
    
    class Dataset < Sequel::Dataset
      BOOL_TRUE = '1'.freeze
      BOOL_FALSE = '0'.freeze
      ODBC_DATE_FORMAT = "{d '%Y-%m-%d'}".freeze
      TIMESTAMP_FORMAT="{ts '%Y-%m-%d %H:%M:%S'}".freeze

      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |s|
          i = -1
          cols = s.columns(true).map{|c| [output_identifier(c.name), i+=1]}
          columns = cols.map{|c| c.at(0)}
          @columns = columns
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
          db.to_application_timestamp([v.year, v.month, v.day, v.hour, v.minute, v.second, v.fraction])
        when ::ODBC::Time
          Sequel::SQLTime.create(v.hour, v.minute, v.second)
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
