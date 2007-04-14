if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'sqlite3'
require 'metaid'

module Sequel
  module SQLite
    class Database < Sequel::Database
      set_adapter_scheme :sqlite
    
      def initialize(opts = {})
        super
        @pool.connection_proc = proc do
          db = SQLite3::Database.new(@opts[:database])
          db.type_translation = true
          db
        end
      end
    
      def dataset(opts = nil)
        SQLite::Dataset.new(self, opts)
      end
      
      TABLES_FILTER = "type = 'table' AND NOT name = 'sqlite_sequence'"
    
      def tables
        self[:sqlite_master].filter(TABLES_FILTER).map {|r| r[:name].to_sym}
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.execute(sql)}
      end
      
      def execute_insert(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.execute(sql); conn.last_insert_row_id}
      end
      
      def single_value(sql)
        @pool.hold {|conn| conn.get_first_value(sql)}
      end
      
      def result_set(sql, record_class, &block)
        @pool.hold do |conn|
          conn.query(sql) do |result|
            columns = result.columns
            column_count = columns.size
            result.each do |values|
              row = {}
              column_count.times {|i| row[columns[i].to_sym] = values[i]}
              block.call(record_class ? record_class.new(row) : row)
            end
          end
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
        @db.result_set(select_sql(opts), @record_class, &block)
        self
      end
    
      LIMIT_1 = {:limit => 1}.freeze
    
      def first_record(opts = nil)
        @db.result_set(select_sql(opts), @record_class) {|r| return r}
      end
    
      def count(opts = nil)
        @db.single_value(count_sql(opts)).to_i
      end
    
      def insert(*values)
        @db.synchronize do
          @db.execute_insert insert_sql(*values)
        end
      end
    
      def update(values, opts = nil)
        @db.synchronize do
          @db.execute update_sql(values, opts)
        end
        self
      end
    
      def delete(opts = nil)
        @db.synchronize do
          @db.execute delete_sql(opts)
        end
        self
      end
      
      EXPLAIN = 'EXPLAIN %s'.freeze

      def explain
        res = []
        @db.result_set(EXPLAIN % select_sql(opts), nil) {|r| res << r}
        res
      end
    end
  end
end
