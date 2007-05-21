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
    
      def select(sql)
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

      def each(opts = nil, &block)
        @db.synchronize do
          s = @db.select select_sql(opts)
          begin
            fetch_rows(s, &block)
            s.fetch {|r| yield hash_row(s, r)}
          ensure
            s.drop unless s.nil? rescue nil
          end
        end
        self
      end
      
      def fetch_rows(stmt)
        columns = stmt.columns(true)
        rows = stmt.fetch_all
        rows.each {|row| yield hash_row(stmt, columns, row)}
      end
      
      def hash_row(stmt, columns, row)
        hash = {}
        row.each_with_index do |v, idx|
          hash[columns[idx].to_sym] = convert_odbc_value(v)
        end
        hash
      end
      
      def convert_odbc_value(v)
        # When fetching a result set, the Ruby ODBC driver converts all ODBC 
        # SQL types to an equivalent Ruby type; with the exception of
        # SQL_TYPE_DATE, SQL_TYPE_TIME and SQL_TYPE_TIMESTAMP.
        #
        # The conversions below are consistent with the mappings in
        # ODBCColumn#mapSqlTypeToGenericType and Column#klass.
        case v
        when ODBC::TimeStamp
          Time.gm(v.year, v.month, v.day, v.hour, v.minute, v.second)
        when ODBC::Time
          DateTime.now
          Time.gm(now.year, now.month, now.day, v.hour, v.minute, v.second)
        when ODBC::Date
          Date.new(v.year, v.month, v.day)
        else
          v
        end
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