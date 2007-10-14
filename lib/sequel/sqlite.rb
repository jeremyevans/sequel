if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'sqlite3'

module Sequel
  module SQLite
    class Database < Sequel::Database
      set_adapter_scheme :sqlite
    
      def serial_primary_key_options
        {:primary_key => true, :type => :integer, :auto_increment => true}
      end

      def connect
        if @opts[:database].nil? || @opts[:database].empty?
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
        @pool.hold {|conn| conn.execute_batch(sql); conn.changes}
      end
      
      def execute_insert(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.execute(sql); conn.last_insert_row_id}
      end
      
      def single_value(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.get_first_value(sql)}
      end
      
      def execute_select(sql, &block)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.query(sql, &block)}
      end
      
      def pragma_get(name)
        single_value("PRAGMA #{name};")
      end
      
      def pragma_set(name, value)
        execute("PRAGMA #{name} = #{value};")
      end
      
      AUTO_VACUUM = {'0' => :none, '1' => :full, '2' => :incremental}.freeze
      
      def auto_vacuum
        AUTO_VACUUM[pragma_get(:auto_vacuum)]
      end
      
      def auto_vacuum=(value)
        value = AUTO_VACUUM.index(value) || (raise SequelError, "Invalid value for auto_vacuum option. Please specify one of :none, :full, :incremental.")
        pragma_set(:auto_vacuum, value)
      end
      
      SYNCHRONOUS = {'0' => :off, '1' => :normal, '2' => :full}.freeze
      
      def synchronous
        SYNCHRONOUS[pragma_get(:synchronous)]
      end
      
      def synchronous=(value)
        value = SYNCHRONOUS.index(value) || (raise SequelError, "Invalid value for synchronous option. Please specify one of :off, :normal, :full.")
        pragma_set(:synchronous, value)
      end
      
      TEMP_STORE = {'0' => :default, '1' => :file, '2' => :memory}.freeze
      
      def temp_store
        TEMP_STORE[pragma_get(:temp_store)]
      end
      
      def temp_store=(value)
        value = TEMP_STORE.index(value) || (raise SequelError, "Invalid value for temp_store option. Please specify one of :default, :file, :memory.")
        pragma_set(:temp_store, value)
      end
      
      def transaction(&block)
        @pool.hold do |conn|
          if conn.transaction_active?
            return yield(conn)
          end
          begin
            result = nil
            conn.transaction {result = yield(conn)}
            result
          rescue => e
            raise e unless SequelRollbackError === e
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

      def fetch_rows(sql, &block)
        @db.execute_select(sql) do |result|
          @columns = result.columns.map {|c| c.to_sym}
          result.each(&block)
          # column_count = @columns.size
          # result.each do |values|
          #   row = {}
          #   column_count.times {|i| row[@columns[i]] = values[i]}
          #   block.call(row)
          # end
        end
      end
    
      def insert(*values)
        @db.execute_insert insert_sql(*values)
      end
    
      def update(values, opts = nil)
        @db.execute update_sql(values, opts)
      end
    
      def delete(opts = nil)
        # check if no filter is specified
        unless (opts && opts[:where]) || @opts[:where]
          @db.transaction do
            unfiltered_count = count
            @db.execute delete_sql(opts)
            unfiltered_count
          end
        else
          @db.execute delete_sql(opts)
        end
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
