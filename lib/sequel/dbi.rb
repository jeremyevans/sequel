if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'dbi'

module Sequel
  module DBI

    class Database < Sequel::Database
      set_adapter_scheme :dbi
    
      def connect
        dbname = @opts[:database] =~ /^DBI:/ ? \
          @opts[:database] : @opts[:database] = 'DBI:' + @opts[:database]
        end
        DBI.connect(dbname, @opts[:user], @opts[:password])
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

      def each(opts = nil, &block)
        @db.synchronize do
          s = @db.execute select_sql(opts)
          s.fetch {|r| yield hash_row(s, r)}
        end
        self
      end
      
      def hash_row(stmt, row)
        stmt.column_names.inject({}) do |m, n|
          m[n.to_sym] = row.shift
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