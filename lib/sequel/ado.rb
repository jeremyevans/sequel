if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'win32ole'

module Sequel
  module ADO
    class Database < Sequel::Database
      set_adapter_scheme :ado
      
      def connect
        dbname = @opts[:database]
        handle = WIN32OLE.new('ADODB.Connection')
        handle.Open(dbname)
        handle
      end
    
      def dataset(opts = nil)
        ADO::Dataset.new(self, opts)
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.Execute(sql)}
      end
      
      alias_method :do, :execute
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
          
          num_cols = s.Fields.Count
          @columns = Array.new(num_cols)
          0.upto(num_cols-1) {|x| @columns[x] = s.Fields(x).Name.to_sym}
          
          s.getRows.transpose.each {|r| yield hash_row(r) }
        end
        self
      end
      
      def hash_row(row)
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