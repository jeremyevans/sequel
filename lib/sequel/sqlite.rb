if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'sqlite3'
require 'metaid'

module Sequel
  module SQLite
    class Database < Sequel::Database
      set_adapter_scheme :sqlite
    
      def connect
        if @opts[:database].empty?
          @opts[:database] = ':memory:'
        end
        db = ::SQLite3::Database.new(@opts[:database])
        db.type_translation = true
        db
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
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.get_first_value(sql)}
      end
      
      def result_set(sql, model_class, &block)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.query(sql) do |result|
            columns = result.columns
            column_count = columns.size
            result.each do |values|
              row = {}
              column_count.times {|i| row[columns[i].to_sym] = values[i]}
              block.call(model_class ? model_class.new(row) : row)
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
        @db.result_set(select_sql(opts), @model_class, &block)
        self
      end
    
      def insert(*values)
        @db.execute_insert insert_sql(*values)
      end
    
      def update(values, opts = nil)
        @db.execute update_sql(values, opts)
        self
      end
    
      def delete(opts = nil)
        @db.execute delete_sql(opts)
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
