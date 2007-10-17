if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'dbi'

module Sequel
  module DBI
    class Database < Sequel::Database
      set_adapter_scheme :dbi
    
      def connect
        dbname = @opts[:database]
        dbname = 'DBI:' + dbname unless dbname =~ /^DBI:/
        ::DBI.connect(dbname, @opts[:user], @opts[:password])
      end
      
      def disconnect
        @pool.disconnect {|c| c.disconnect}
      end
    
      def dataset(opts = nil)
        DBI::Dataset.new(self, opts)
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.execute(sql)
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
        when Time: literal(v.iso8601)
        else
          super
        end
      end

      def fetch_rows(sql, &block)
        @db.synchronize do
          s = @db.execute sql
          begin
            @columns = s.column_names.map {|c| c.to_sym}
            s.fetch {|r| yield hash_row(s, r)}
          ensure
            s.finish rescue nil
          end
        end
        self
      end
      
      def array_tuples_fetch_rows(sql, &block)
        @db.synchronize do
          s = @db.execute sql
          begin
            @columns = s.column_names.map {|c| c.to_sym}
            s.fetch {|r| r.fields = @columns; yield r}
          ensure
            s.finish rescue nil
          end
        end
        self
      end
      
      def hash_row(stmt, row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
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