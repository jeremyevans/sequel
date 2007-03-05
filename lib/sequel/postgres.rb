require 'postgres'

class PGconn
  # the pure-ruby postgres adapter does not have a quote method.
  unless methods.include?('quote')
    def self.quote(obj)
      case obj
      when true: 't'
      when false: 'f'
      when nil: 'NULL'
      when String: "'#{obj}'"
      else obj.to_s
      end
    end
  end
  
  def connected?
    status == PGconn::CONNECTION_OK
  end
  
  SQL_BEGIN = 'BEGIN'.freeze
  SQL_COMMIT = 'COMMIT'.freeze
  SQL_ROLLBACK = 'ROLLBACK'.freeze
  
  def execute(sql)
    begin
      # ServerSide.info(sql)
      async_exec(sql)
    rescue PGError => e
      unless connected?
        # ServerSide.warn('Reconnecting to Postgres server')
        reset
        async_exec(sql)
      else
        p sql
        p e
        raise e
      end
    end
  end
  
  attr_reader :transaction_in_progress
  
  def transaction
    if @transaction_in_progress
      return yield
    end
    # ServerSide.info('BEGIN')
    async_exec(SQL_BEGIN)
    begin
      @transaction_in_progress = true
      result = yield
      # ServerSide.info('COMMIT')
      async_exec(SQL_COMMIT)
      result
    rescue => e
      # ServerSide.info('ROLLBACK')
      async_exec(SQL_ROLLBACK)
      raise e
    ensure
      @transaction_in_progress = nil
    end
  end
end

class String
  def postgres_to_bool
    if self == 't'
      true
    elsif self == 'f'
      false
    else
      nil
    end
  end

  TIME_REGEXP = /(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})/
  
  def postgres_to_time
    if self =~ TIME_REGEXP
      Time.local($1.to_i, $2.to_i, $3.to_i, $4.to_i, $5.to_i, $6.to_i)
    else
      nil
    end
  end
end

module Sequel
  module Postgres
    PG_TYPES = {
      16 => :postgres_to_bool,
      20 => :to_i,
      21 => :to_i,
      22 => :to_i,
      23 => :to_i,
      700 => :to_f,
      701 => :to_f,
      1114 => :postgres_to_time
    }

    class Database < Sequel::Database
      set_adapter_scheme :postgres
    
      attr_reader :pool
    
      def initialize(opts = {})
        super
        @pool = ConnectionPool.new(@opts[:max_connections] || 4) do
          PGconn.connect(
            @opts[:host] || 'localhost',
            @opts[:port] || 5432,
            '', '',
            @opts[:database] || 'reality_development',
            @opts[:user] || 'postgres',
            @opts[:password])
        end
      end
    
    
      def dataset(opts = nil)
        Postgres::Dataset.new(self, opts)
      end
    
      RELATION_QUERY = {:from => :pg_class, :select => :relname}.freeze
      RELATION_FILTER = "(relkind = 'r') AND (relname !~ '^pg|sql')".freeze
      SYSTEM_TABLE_REGEXP = /^pg|sql/.freeze
    
    
      def tables
        query(RELATION_QUERY).filter(RELATION_FILTER).map(:relname)
      end
      
      def locks
        query.from("pg_class, pg_locks").
          select("pg_class.relname, pg_locks.*").
          filter("pg_class.relfilenode=pg_locks.relation")
      end
    
      def execute(sql)
        @pool.hold {|conn| conn.execute(sql)}
      end
    
      def execute_and_forget(sql)
        @pool.hold {|conn| conn.execute(sql).clear}
      end
    
      def synchronize(&block)
        @pool.hold(&block)
      end
      
      def transaction(&block)
        @pool.hold {|conn| conn.transaction(&block)}
      end

      def table_exists?(name)
        from(:pg_class).filter(:relname => name, :relkind => 'r').count > 0
      end
    end
  
    class Dataset < Sequel::Dataset
      attr_reader :result, :fields
  
      def literal(v)
        case v
        when Time: v.to_sql_timestamp
        when Symbol: PGconn.quote(v.to_s)
        when Array: v.empty? ? EMPTY_ARRAY : v.join(COMMA_SEPARATOR)
        else
          PGconn.quote(v)
        end
      end
    
      LIKE = '%s ~ %s'.freeze
      LIKE_CI = '%s ~* %s'.freeze
    
      IN_ARRAY = '%s IN (%s)'.freeze
      EMPTY_ARRAY = 'NULL'.freeze
    
      def where_equal_condition(left, right)
        case right
        when Regexp:
          (right.casefold? ? LIKE_CI : LIKE) %
            [field_name(left), PGconn.quote(right.source)]
        when Array:
          IN_ARRAY % [field_name(left), literal(right)]
        else
          super
        end
      end
    
      def each(opts = nil, &block)
        query_each(select_sql(opts), true, &block)
        self
      end
    
      LIMIT_1 = {:limit => 1}.freeze
    
      def first(opts = nil)
        opts = opts ? opts.merge(LIMIT_1) : LIMIT_1
        query_first(select_sql(opts), true)
      end
    
      def last(opts = nil)
        raise RuntimeError, 'No order specified' unless
          @opts[:order] || (opts && opts[:order])
      
        opts = {:order => reverse_order(@opts[:order])}.
          merge(opts ? opts.merge(LIMIT_1) : LIMIT_1)
      
        query_first(select_sql(opts), true)
      end
    
      FOR_UPDATE = ' FOR UPDATE'.freeze
      FOR_SHARE = ' FOR SHARE'.freeze
    
      def select_sql(opts = nil)
        row_lock_mode = opts ? opts[:lock] : @opts[:lock]
        sql = super
        case row_lock_mode
        when :update : sql << FOR_UPDATE
        when :share  : sql << FOR_SHARE
        end
        sql
      end
    
      def for_update
        dup_merge(:lock => :update)
      end
    
      def for_share
        dup_merge(:lock => :share)
      end
    
      EXPLAIN = 'EXPLAIN '.freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
    
      def explain(opts = nil)
        analysis = []
        query_each(select_sql(EXPLAIN + select_sql(opts))) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
    
      LOCK = 'LOCK TABLE %s IN %s MODE;'.freeze
    
      ACCESS_SHARE = 'ACCESS SHARE'.freeze
      ROW_SHARE = 'ROW SHARE'.freeze
      ROW_EXCLUSIVE = 'ROW EXCLUSIVE'.freeze
      SHARE_UPDATE_EXCLUSIVE = 'SHARE UPDATE EXCLUSIVE'.freeze
      SHARE = 'SHARE'.freeze
      SHARE_ROW_EXCLUSIVE = 'SHARE ROW EXCLUSIVE'.freeze
      EXCLUSIVE = 'EXCLUSIVE'.freeze
      ACCESS_EXCLUSIVE = 'ACCESS EXCLUSIVE'.freeze
    
      # Locks the table with the specified mode.
      def lock(mode, &block)
        sql = LOCK % [@opts[:from], mode]
        @db.synchronize do
          if block # perform locking inside a transaction and yield to block
            @db.transaction {@db.execute_and_forget(sql); yield}
          else
            @db.execute_and_forget(sql) # lock without a transaction
            self
          end
        end
      end
  
      def count(opts = nil)
        query_single_value(count_sql(opts)).to_i
      end
    
      SELECT_LASTVAL = ';SELECT lastval()'.freeze
    
      def insert(values = nil, opts = nil)
        @db.execute_and_forget(insert_sql(values, opts))
        query_single_value(SELECT_LASTVAL).to_i
      end
    
      def update(values, opts = nil)
        @db.synchronize do
          result = @db.execute(update_sql(values))
          begin
            affected = result.cmdtuples
          ensure
            result.clear
          end
          affected
        end
      end
    
      def delete(opts = nil)
        @db.synchronize do
          result = @db.execute(delete_sql(opts))
          begin
            affected = result.cmdtuples
          ensure
            result.clear
          end
          affected
        end
      end
      
      def query_all(sql, use_record_class = false)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            conv = row_converter(result, use_record_class)
            all = []
            result.each {|r| all << conv[r]}
          ensure
            result.clear
          end
          all
        end
      end
    
      def query_each(sql, use_record_class = false)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            conv = row_converter(result, use_record_class)
            result.each {|r| yield conv[r]}
          ensure
            result.clear
          end
        end
      end
      
      def query_first(sql, use_record_class = false)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            row = nil
            conv = row_converter(result, use_record_class)
            result.each {|r| row = conv.call(r)}
          ensure
            result.clear
          end
          row
        end
      end
      
      def query_single_value(sql)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            value = result.getvalue(0, 0)
          ensure
            result.clear
          end
          value
        end
      end
    
      COMMA = ','.freeze
    
      @@converters_mutex = Mutex.new
      @@converters = {}

      def row_converter(result, use_record_class)
        fields = result.fields.map {|s| s.to_sym}
        types = (0..(result.num_fields - 1)).map {|idx| result.type(idx)}
        klass = use_record_class ? @record_class : nil
        
        # create result signature and memoize the converter
        sig = fields.join(COMMA) + types.join(COMMA) + klass.to_s
        @@converters_mutex.synchronize do
          @@converters[sig] ||= compile_converter(fields, types, klass)
        end
      end
    
      CONVERT = "lambda {|r| {%s}}".freeze
      CONVERT_RECORD_CLASS = "lambda {|r| %2$s.new(%1$s)}".freeze
    
      CONVERT_FIELD = '%s => r[%d]'.freeze
      CONVERT_FIELD_TRANSLATE = '%s => ((t = r[%d]) ? t.%s : nil)'.freeze

      def compile_converter(fields, types, klass)
        used_fields = []
        kvs = []
        fields.each_with_index do |field, idx|
          next if used_fields.include?(field)
          used_fields << field
        
          translate_fn = PG_TYPES[types[idx]]
          kvs << (translate_fn ? CONVERT_FIELD_TRANSLATE : CONVERT_FIELD) %
            [field.inspect, idx, translate_fn]
        end
        s = (klass ? CONVERT_RECORD_CLASS : CONVERT) %
          [kvs.join(COMMA), klass]
        eval(s)
      end
    end
  end
end
