if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

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
  
  SQL_BEGIN = 'BEGIN'.freeze
  SQL_COMMIT = 'COMMIT'.freeze
  SQL_ROLLBACK = 'ROLLBACK'.freeze
  
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

  SELECT_CURRVAL = "SELECT currval('%s')".freeze
      
  def last_insert_id(table)
    @table_sequences ||= {}
    seq = @table_sequences[table] ||= pkey_and_sequence(table)[1]
    r = async_query(SELECT_CURRVAL % seq)
    r[0][0].to_i unless r.nil? || r.empty?
  end
      
  # Shamelessly appropriated from ActiveRecord's Postgresql adapter.
  
  SELECT_PK_AND_SERIAL_SEQUENCE = <<-end_sql
    SELECT attr.attname, name.nspname, seq.relname
    FROM pg_class seq, pg_attribute attr, pg_depend dep,
      pg_namespace name, pg_constraint cons
    WHERE seq.oid = dep.objid
      AND seq.relnamespace  = name.oid
      AND seq.relkind = 'S'
      AND attr.attrelid = dep.refobjid
      AND attr.attnum = dep.refobjsubid
      AND attr.attrelid = cons.conrelid
      AND attr.attnum = cons.conkey[1]
      AND cons.contype = 'p'
      AND dep.refobjid = '%s'::regclass
  end_sql
  
  SELECT_PK_AND_CUSTOM_SEQUENCE = <<-end_sql
    SELECT attr.attname, name.nspname, split_part(def.adsrc, '''', 2)
    FROM pg_class t
    JOIN pg_namespace  name ON (t.relnamespace = name.oid)
    JOIN pg_attribute  attr ON (t.oid = attrelid)
    JOIN pg_attrdef    def  ON (adrelid = attrelid AND adnum = attnum)
    JOIN pg_constraint cons ON (conrelid = adrelid AND adnum = conkey[1])
    WHERE t.oid = '%s'::regclass
      AND cons.contype = 'p'
      AND def.adsrc ~* 'nextval'
  end_sql
  
  def pkey_and_sequence(table)
    r = async_query(SELECT_PK_AND_SERIAL_SEQUENCE % table)
    return [r[0].first, r[0].last] unless r.nil? or r.empty?

    r = async_query(SELECT_PK_AND_CUSTOM_SEQUENCE % table)
    return [r.first, r.last] unless r.nil? or r.empty?
  rescue
    nil
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
    
      def initialize(opts = {})
        super
        @pool.connection_proc = proc do
          PGconn.connect(
            @opts[:host] || 'localhost',
            @opts[:port] || 5432,
            '', '',
            @opts[:database] || 'reality_development',
            @opts[:user] || 'postgres',
            @opts[:password]
          )
        end
      end
    
    
      def dataset(opts = nil)
        Postgres::Dataset.new(self, opts)
      end
    
      RELATION_QUERY = {:from => [:pg_class], :select => [:relname]}.freeze
      RELATION_FILTER = "(relkind = 'r') AND (relname !~ '^pg|sql')".freeze
      SYSTEM_TABLE_REGEXP = /^pg|sql/.freeze
    
    
      def tables
        dataset(RELATION_QUERY).filter(RELATION_FILTER).map {|r| r[:relname].to_sym}
      end
      
      def locks
        dataset.from("pg_class, pg_locks").
          select("pg_class.relname, pg_locks.*").
          filter("pg_class.relfilenode=pg_locks.relation")
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.execute(sql)}
      end
    
      def execute_and_forget(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.execute(sql).clear}
      end
      
      def execute_insert(sql, table)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.execute(sql).clear
          conn.last_insert_id(table)
        end
      end
    
      def synchronize(&block)
        @pool.hold(&block)
      end
      
      def transaction(&block)
        @pool.hold {|conn| conn.transaction(&block)}
      end
    end
  
    class Dataset < Sequel::Dataset
      TRUE = "'t'".freeze
      FALSE = "'f'".freeze
      
      def literal(v)
        case v
        # when String: "'%s'" % v.gsub(/'/, "''")
        # when Integer, Float: v.to_s
        # when NilClass: NULL
        # when Symbol: v.to_field_name
        # when Array: v.empty? ? NULL : v.map {|i| literal(i)}.join(COMMA_SEPARATOR)
        # when Time: v.strftime(TIMESTAMP_FORMAT)
        # when Date: v.strftime(DATE_FORMAT)
        # when Dataset: "(#{v.sql})"
        # when true: TRUE
        # when false: FALSE
        # else
        #   raise SequelError, "can't express #{v.inspect}:#{v.class} as a SQL literal"
        # end
        when String, Fixnum, Float, TrueClass, FalseClass: PGconn.quote(v)
        else
          super
        end
      end
    
      LIKE = '%s ~ %s'.freeze
      LIKE_CI = '%s ~* %s'.freeze
      
      def format_eq_expression(left, right)
        case right
        when Regexp:
          (right.casefold? ? LIKE_CI : LIKE) %
            [field_name(left), PGconn.quote(right.source)]
        else
          super
        end
      end
      
      def each(opts = nil, &block)
        query_each(select_sql(opts), true, &block)
        self
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
  
      def insert(*values)
        @db.execute_insert(insert_sql(*values), @opts[:from])
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
      
      def single_record(opts = nil)
        query_single(select_sql(opts), true)
      end
      
      def single_value(opts = nil)
        query_single_value(select_sql(opts))
      end
      
      def query_each(sql, use_model_class = false, &block)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            each_row(result, use_model_class, &block)
            # conv = row_converter(result, use_model_class)
            # result.each {|r| yield conv[r]}
          ensure
            result.clear
          end
        end
      end
      
      def query_single(sql, use_model_class = false)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            row = nil
            each_row(result, use_model_class) {|r| row = r}
            # conv = row_converter(result, use_model_class)
            # result.each {|r| row = conv.call(r)}
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
            if value
              value = value.send(PG_TYPES[result.type(0)])
            end
          ensure
            result.clear
          end
          value
        end
      end
      
      def each_row(result, use_model_class)
        fields = result.fields.map {|s| s.to_sym}
        types = (0..(result.num_fields - 1)).map {|idx| PG_TYPES[result.type(idx)]}
        m_klass = use_model_class && @model_class
        result.each do |row|
          hashed_row = {}
          row.each_index do |cel_index|
            column = row[cel_index]
            if column && types[cel_index]
              column = column.send(types[cel_index])
            end
            hashed_row[fields[cel_index]] = column
          end
          yield m_klass ? m_klass.new(hashed_row) : hashed_row
        end
      end
    
      COMMA = ','.freeze
    
      @@converters_mutex = Mutex.new
      @@converters = {}

      def row_converter(result, use_model_class)
        fields = result.fields.map {|s| s.to_sym}
        types = (0..(result.num_fields - 1)).map {|idx| result.type(idx)}
        klass = use_model_class ? @model_class : nil
        
        # create result signature and memoize the converter
        sig = fields.join(COMMA) + types.join(COMMA) + klass.to_s
        @@converters_mutex.synchronize do
          @@converters[sig] ||= compile_converter(fields, types, klass)
        end
      end
    
      CONVERT = "lambda {|r| {%s}}".freeze
      CONVERT_MODEL_CLASS = "lambda {|r| %2$s.new(%1$s)}".freeze
    
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
        s = (klass ? CONVERT_MODEL_CLASS : CONVERT) %
          [kvs.join(COMMA), klass]
        eval(s)
      end
    end
  end
end
