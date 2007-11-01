if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'win32ole'

module Sequel
  # The ADO adapter provides connectivity to ADO databases in Windows. ADO
  # databases can be opened using a URL with the ado schema:
  #
  #   DB = Sequel.open('ado://mydb')
  # 
  # or using the Sequel.ado method:
  #
  #   DB = Sequel.ado('mydb')
  #
  module ADO
    class Database < Sequel::Database
      set_adapter_scheme :ado
      
      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      
      def auto_increment_sql
        AUTO_INCREMENT
      end
      
      def connect
        dbname = @opts[:database]
        handle = WIN32OLE.new('ADODB.Connection')
        handle.Open(dbname)
        handle
      end
      
      def disconnect
        # how do we disconnect? couldn't find anything in the docs
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
          
          @columns = s.Fields.extend(Enumerable).map {|x| x.Name.to_sym}
          
          s.moveFirst
          s.getRows.transpose.each {|r| yield hash_row(r)}
        end
        self
      end
      
      def hash_row(row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
        end
      end
    
      def array_tuples_fetch_rows(sql, &block)
        @db.synchronize do
          s = @db.execute sql
          
          @columns = s.Fields.extend(Enumerable).map {|x| x.Name.to_sym}
          
          s.moveFirst
          s.getRows.transpose.each {|r| r.keys = @columns; yield r}
        end
        self
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