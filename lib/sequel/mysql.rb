if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'mysql'

# Monkey patch Mysql::Result to return a hash with symbol keys
class Mysql::Result
  def fetch_hash(with_table=nil)
    row = fetch_row
    return if row == nil
    hash = {}
    @fields.each_index do |i|
      f = with_table ? @fields[i].table+"."+@fields[i].name : @fields[i].name
      hash[f.to_sym] = row[i]
    end
    hash
  end
end

module Sequel
  module MySQL

    class Database < Sequel::Database
      set_adapter_scheme :mysql
    
      def connect
        Mysql.real_connect(@opts[:host], @opts[:user], @opts[:password], 
          @opts[:database], @opts[:port])
      end
    
      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.query(sql)
        end
      end
      
      def execute_insert(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.query(sql)
          conn.insert_id
        end
      end
    
      def execute_affected(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.query(sql)
          conn.affected_rows
        end
      end
    
      def transaction(&block)
        @pool.hold {|conn| conn.transaction(&block)}
      end
    end
    
    class Dataset < Sequel::Dataset
      def insert(*values)
        @db.execute_insert(insert_sql(*values))
      end
    
      def update(values, opts = nil)
        @db.execute_affected(update_sql(values, opts))
      end
    
      def delete(opts = nil)
        @db.execute_affected(delete_sql(opts))
      end
      
      def fetch_rows(sql)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            fetch_columns(result)
            result.each_hash {|r| yield r}
          ensure
            result.free
          end
        end
        self
      end
      
      def fetch_columns(result)
        @columns = result.fetch_fields.map {|c| c.name.to_sym}
      end
    end
  end
end