if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'mysql'

module Sequel
  module MySQL

    class Database < Sequel::Database
      set_adapter_scheme :mysql
    
      def initialize(opts = {})
        super
        @pool.connection_proc = proc do
          Mysql.real_connect(@opts[:host], @opts[:user], @opts[:password], 
            @opts[:database], @opts[:port])
        end
      end
    
      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end
    
      def execute(sql)
        @pool.hold do |conn|
          conn.query(sql)
        end
      end
      
      def execute_insert(sql)
        @pool.hold do |conn|
          conn.query(sql)
          conn.insert_id
        end
      end
    
      def execute_affected(sql)
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
      def each(opts = nil, &block)
        query_each(select_sql(opts), true, &block)
        self
      end
      
      def first_record(opts = nil)
        query_first(select_sql(opts), true)
      end
    
      def count(opts = nil)
        query_single_value(count_sql(opts)).to_i
      end
    
      def insert(*values)
        @db.execute_insert(insert_sql(*values))
      end
    
      def update(values, opts = nil)
        @db.execute_affected(update_sql(values, opts))
      end
    
      def delete(opts = nil)
        @db.execute_affected(delete_sql(opts))
      end
      
      def query_each(sql, use_record_class = false)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            if use_record_class && @record_class
              result.each_hash {|r| yield @record_class.new(r)}
            else
              result.each_hash {|r| yield r}
            end
          ensure
            result.free
          end
        end
        self
      end
      
      def query_first(sql, use_record_class = false)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            if use_record_class && @record_class
              @record_class.new(result.fetch_hash)
            else
              result.fetch_hash
            end
          ensure
            result.free
          end
          row
        end
      end
      
      def query_single_value(sql)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            return result.fetch_hash.values[0]
          ensure
            result.free
          end
        end
      end
    end
  end
end