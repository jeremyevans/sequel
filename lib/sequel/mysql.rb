if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'mysql'

# Monkey patch Mysql::Result to yield hashes with symbol keys
class Mysql::Result
  def columns(with_table = nil)
    @columns ||= fetch_fields.map do |f|
      (with_table ? (f.table + "." + f.name) : f.name).to_sym
    end
  end
  
  def each_hash(with_table=nil)
    c = columns
    while row = fetch_row
      h = {}
      c.each_with_index {|f, i| h[f] = row[i]}
      yield h
    end
  end
end

class Mysql::Stmt
  def columns(with_table = nil)
    @columns ||= result_metadata.fetch_fields.map do |f|
      (with_table ? (f.table + "." + f.name) : f.name).to_sym
    end
  end
  
  def each_hash
    c = columns
    while row = fetch
      h = {}
      c.each_with_index {|f, i| h[f] = row[i]}
      yield h
    end
  end
end

module Sequel
  module MySQL
    class Database < Sequel::Database
      set_adapter_scheme :mysql
    
      def connect
        conn = Mysql.real_connect(@opts[:host], @opts[:user], @opts[:password], 
          @opts[:database], @opts[:port])
        conn.query_with_result = false
        conn
      end
      
      def tables
        @pool.hold do |conn|
          conn.list_tables.map {|t| t.to_sym}
        end
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
      
      def query(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.query(sql)
          conn.use_result
        end
      end
      
      def stmt(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          stmt = conn.prepare(sql)
          stmt.execute
          stmt
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

      def transaction
        @pool.hold do |conn|
          @transactions ||= []
          if @transactions.include? Thread.current
            return yield(conn)
          end
          conn.query(SQL_BEGIN)
          begin
            @transactions << Thread.current
            result = yield(conn)
            conn.query(SQL_COMMIT)
            result
          rescue => e
            conn.query(SQL_ROLLBACK)
            raise e
          ensure
            @transactions.delete(Thread.current)
          end
        end
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
          s = @db.stmt(sql)
          begin
            @columns = s.columns
            s.each_hash {|r| yield r}
          ensure
            s.close
          end
        end
        self
      end
    end
  end
end