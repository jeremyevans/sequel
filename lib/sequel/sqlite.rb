if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'sqlite3'

class String
  def sqlite_to_bool
    !(strip.gsub(/00+/,"0") == "0" ||
      downcase == "false" ||
      downcase == "f" ||
      downcase == "no" ||
      downcase == "n")
  end
end

module SQLite3
  class ResultSet
    SQLITE_TYPES = {
      :date => :to_time,
      :datetime => :to_time,
      :time => :to_time,
      :timestamp => :to_time,
      
      :decimal => :to_f,
      :float => :to_f,
      :numeric => :to_f,
      :double => :to_f,
      :real => :to_f,
      :dec => :to_f,
      :fixed => :to_f,
      
      :integer => :to_i,
      :smallint => :to_i,
      :mediumint => :to_i,
      :int => :to_i,
      :bigint => :to_i,
      
      :bit => :sqlite_to_bool,
      :bool => :sqlite_to_bool,
      :boolean => :sqlite_to_bool,
      
      :tinyint => :to_i
    }
    
    COMMA_SEPARATOR = ', '.freeze
    
    @@fetchers_mutex = Mutex.new
    @@fetchers = {}

    def prepare_row_fetcher
      column_count = @driver.data_count(@stmt.handle)
      columns = @stmt.columns.map {|c| c.to_sym}
      translators = []
      column_count.times do |idx|
        t = @driver.column_decltype(@stmt.handle, idx) || :text
        translators << SQLITE_TYPES[t.to_sym]
      end
      sig = [columns, translators].hash
      @@fetchers_mutex.synchronize do
        fetcher = (@@fetchers[sig] ||= compile_fetcher(columns, translators))
        meta_def(:fetch_hash, &fetcher)
      end
    end
  
    def compile_fetcher(columns, translators)
      used_columns = []
      kvs = []
      columns.each_with_index do |column, idx|
        next if used_columns.include?(column)
        used_columns << column
      
        if translator = translators[idx]
          kvs << "#{column.inspect} => ((t = @driver.column_text(@stmt.handle, #{idx})) ? t.#{translator} : nil)"
        else
          kvs << "#{column.inspect} => @driver.column_text(@stmt.handle, #{idx})"
        end
      end
      eval("lambda {{#{kvs.join(COMMA_SEPARATOR)}}}")
    end


    def next_hash_tuple
      return nil if @eof
      @stmt.must_be_open!

      if @first_row
        prepare_row_fetcher
        # @row_fetcher = prepare_row_fetcher
      else
        result = @driver.step(@stmt.handle)
        check result
      end
      @first_row = false
      
      @eof ? nil : fetch_hash # @row_fetcher.call
    end

    def next_array_tuple
      return nil if @eof
      @stmt.must_be_open!

      unless @first_row
        result = @driver.step(@stmt.handle)
        check result
      end
      @first_row = false

      columns = @stmt.columns

      unless @eof
        row = []
        @driver.data_count( @stmt.handle ).times do |idx|
          case @driver.column_type( @stmt.handle, idx )
          when Constants::ColumnType::NULL then
            row << nil
          when Constants::ColumnType::BLOB then
            row << @driver.column_blob( @stmt.handle, idx )
          else
            v = @driver.column_text( @stmt.handle, idx )
            row << @db.translator.translate(@driver.column_decltype(@stmt.handle, idx), v)
          end
        end

        row.extend FieldsContainer unless row.respond_to?(:fields)
        row.fields = @stmt.columns

        row.extend TypesContainer
        row.types = @stmt.types

        return row
      end

      nil
    end

    def each_hash
      while row=self.next_hash_tuple
        yield row
      end
    end

    def each_array
      while row=self.next_array_tuple
        yield row
      end
    end
  end
end

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
      
      def disconnect
        @pool.disconnect {|c| c.close}
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

      def insert_sql(*values)
        if (values.size == 1) && values.first.is_a?(Sequel::Dataset)
          "INSERT INTO #{@opts[:from]} #{values.first.sql};"
        else
          super(*values)
        end
      end

      def fetch_rows(sql, &block)
        @db.execute_select(sql) do |result|
          @columns = result.columns.map {|c| c.to_sym}
          result.each_hash(&block)
        end
      end
      
      def array_tuples_fetch_rows(sql, &block)
        @db.execute_select(sql) do |result|
          @columns = result.columns.map {|c| c.to_sym}
          result.each_array(&block)
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
