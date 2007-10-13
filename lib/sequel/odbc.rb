if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'odbc'

module Sequel
  module ODBC
    class Database < Sequel::Database
      set_adapter_scheme :odbc
    
      def connect
        conn = ::ODBC::connect(@opts[:database], @opts[:user], @opts[:password])
        conn.autocommit = true
        conn
      end
    
      def dataset(opts = nil)
        ODBC::Dataset.new(self, opts)
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.run(sql)
        end
      end
      
      def do(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.do(sql)
        end
      end
    end
    
    class Dataset < Sequel::Dataset
      def literal(v)
        case v
        when true: '1'
        when false: '0'
        else
          super
        end
      end

      def fetch_rows(sql, &block)
        @db.synchronize do
          s = @db.execute sql
          begin
            @columns = s.columns(true).map {|c| c.name.to_sym}
            rows = s.fetch_all
            rows.each {|r| yield make_row(r)}
          ensure
            s.drop unless s.nil? rescue nil
          end
        end
        self
      end
      
      def make_row(row)
        row.fields = @columns
        row.each_with_index do |v, idx|
          # When fetching a result set, the Ruby ODBC driver converts all ODBC 
          # SQL types to an equivalent Ruby type; with the exception of
          # SQL_TYPE_DATE, SQL_TYPE_TIME and SQL_TYPE_TIMESTAMP.
          #
          # The conversions below are consistent with the mappings in
          # ODBCColumn#mapSqlTypeToGenericType and Column#klass.
          case v
          when ::ODBC::TimeStamp
            row[idx] = DateTime.new(v.year, v.month, v.day, v.hour, v.minute, v.second)
          when ::ODBC::Time
            now = DateTime.now
            row[idx] = Time.gm(now.year, now.month, now.day, v.hour, v.minute, v.second)
          when ::ODBC::Date
            row[idx] = Date.new(v.year, v.month, v.day)
          end
        end
        row
      end
      
      def insert(*values)
        @db.do insert_sql(*values)
      end
    
      def update(values, opts = nil)
        @db.do update_sql(values, opts)
        self
      end
    
      def delete(opts = nil)
        @db.do delete_sql(opts)
      end
    end
  end
end