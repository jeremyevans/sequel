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
  
  class << self
    # The postgres gem's string quoting doesn't render string literals properly, which this fixes.
    # 
    #   "a basic string" #=> 'a basic string'
    #   "this\or that"   #=> E'this\\or that'
    # 
    # See <http://www.postgresql.org/docs/8.2/static/sql-syntax-lexical.html> for details.
    def quote_with_proper_escaping(s)
      value = quote_without_proper_escaping(s)
      value = "E#{value}" if value =~ /\\/
      return value
    end
    alias_method :quote_without_proper_escaping, :quote
    alias_method :quote, :quote_with_proper_escaping
  end

  def connected?
    status == PGconn::CONNECTION_OK
  end
  
  def execute(sql)
    begin
      async_exec(sql)
    rescue PGError => e
      unless connected?
        reset
        async_exec(sql)
      else
        raise e
      end
    end
  end
  
  attr_accessor :transaction_in_progress
  
  SELECT_CURRVAL = "SELECT currval('%s')".freeze
      
  def last_insert_id(table)
    @table_sequences ||= {}
    if !@table_sequences.include?(table)
      pkey_and_seq = pkey_and_sequence(table)
      if pkey_and_seq
        @table_sequences[table] = pkey_and_seq[1]
      end
    end
    if seq = @table_sequences[table]
      r = async_query(SELECT_CURRVAL % seq)
      return r[0][0].to_i unless r.nil? || r.empty?
    end
    nil # primary key sequence not found
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

  SELECT_PK = <<-end_sql
    SELECT pg_attribute.attname
    FROM pg_class, pg_attribute, pg_index
    WHERE pg_class.oid = pg_attribute.attrelid AND
      pg_class.oid = pg_index.indrelid AND
      pg_index.indkey[0] = pg_attribute.attnum AND
      pg_index.indisprimary = 't' AND
      pg_class.relname = '%s'
  end_sql
  
  def pkey_and_sequence(table)
    r = async_query(SELECT_PK_AND_SERIAL_SEQUENCE % table)
    return [r[0].first, r[0].last] unless r.nil? or r.empty?

    r = async_query(SELECT_PK_AND_CUSTOM_SEQUENCE % table)
    return [r[0].first, r[0].last] unless r.nil? or r.empty?
  rescue
    nil
  end
  
  def primary_key(table)
    r = async_query(SELECT_PK % table)
    pkey = r[0].first unless r.nil? or r.empty?
    return pkey.to_sym if pkey
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
      1114 => :to_time
    }

    if PGconn.respond_to?(:translate_results=)
      PGconn.translate_results = true
      AUTO_TRANSLATE = true
    else
      AUTO_TRANSLATE = false
    end

    class Database < Sequel::Database
      set_adapter_scheme :postgres
    
      def connect
        conn = PGconn.connect(
          @opts[:host] || 'localhost',
          @opts[:port] || 5432,
          '', '',
          @opts[:database],
          @opts[:user],
          @opts[:password]
        )
        if encoding = @opts[:encoding] || @opts[:charset]
          conn.set_client_encoding(encoding)
        end
        conn
      end
      
      def disconnect
        @pool.disconnect {|c| c.close}
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
      rescue => e
        @logger.error(e.message) if @logger
        raise e
      end
    
      def execute_and_forget(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.execute(sql).clear}
      rescue => e
        @logger.error(e.message) if @logger
        raise e
      end
      
      def primary_key_for_table(conn, table)
        @primary_keys ||= {}
        @primary_keys[table] ||= conn.primary_key(table)
      end
      
      RE_CURRVAL_ERROR = /currval of sequence "(.*)" is not yet defined in this session/.freeze
      
      def insert_result(conn, table, values)
        begin
          result = conn.last_insert_id(table)
          return result if result
        rescue PGError => e
          # An error could occur if the inserted values include a primary key
          # value, while the primary key is serial.
          if e.message =~ RE_CURRVAL_ERROR
            raise SequelError, "Could not return primary key value for the inserted record. Are you specifying a primary key value for a serial primary key?"
          else
            raise e
          end
        end
        
        case values
        when Hash:
          values[primary_key_for_table(conn, table)]
        when Array:
          values.first
        else
          nil
        end
      end
      
      def execute_insert(sql, table, values)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.execute(sql).clear
          insert_result(conn, table, values)
        end
      rescue => e
        @logger.error(e.message) if @logger
        raise e
      end
    
      def synchronize(&block)
        @pool.hold(&block)
      end
      
      SQL_BEGIN = 'BEGIN'.freeze
      SQL_COMMIT = 'COMMIT'.freeze
      SQL_ROLLBACK = 'ROLLBACK'.freeze
  
      def transaction
        @pool.hold do |conn|
          if conn.transaction_in_progress
            yield conn
          else
            @logger.info(SQL_BEGIN) if @logger
            conn.async_exec(SQL_BEGIN)
            begin
              conn.transaction_in_progress = true
              result = yield
              begin
                @logger.info(SQL_COMMIT) if @logger
                conn.async_exec(SQL_COMMIT)
              rescue => e
                @logger.error(e.message) if @logger
                raise e
              end
              result
            rescue => e
              @logger.info(SQL_ROLLBACK) if @logger
              conn.async_exec(SQL_ROLLBACK) rescue nil
              raise e unless SequelRollbackError === e
            ensure
              conn.transaction_in_progress = nil
            end
          end
        end
      end

      def serial_primary_key_options
        {:primary_key => true, :type => :serial}
      end

      def drop_table_sql(name)
        "DROP TABLE #{name} CASCADE"
      end
    end
  
    class Dataset < Sequel::Dataset
      def literal(v)
        case v
        when LiteralString: v
        when String, Fixnum, Float, TrueClass, FalseClass: PGconn.quote(v)
        else
          super
        end
      end
    
      def match_expr(l, r)
        case r
        when Regexp:
          r.casefold? ? \
            "(#{literal(l)} ~* #{literal(r.source)})" :
            "(#{literal(l)} ~ #{literal(r.source)})"
        else
          super
        end
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
        clone_merge(:lock => :update)
      end
    
      def for_share
        clone_merge(:lock => :share)
      end
    
      EXPLAIN = 'EXPLAIN '.freeze
      EXPLAIN_ANALYZE = 'EXPLAIN ANALYZE '.freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
    
      def explain(opts = nil)
        analysis = []
        fetch_rows(EXPLAIN + select_sql(opts)) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
      
      def analyze(opts = nil)
        analysis = []
        fetch_rows(EXPLAIN_ANALYZE + select_sql(opts)) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
    
      LOCK = 'LOCK TABLE %s IN %s MODE'.freeze
    
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
        @db.execute_insert(insert_sql(*values), @opts[:from],
          values.size == 1 ? values.first : values)
      end
    
      def update(*args, &block)
        @db.synchronize do
          result = @db.execute(update_sql(*args, &block))
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
      
      def fetch_rows(sql, &block)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            conv = row_converter(result)
            result.each {|r| yield conv[r]}
          ensure
            result.clear
          end
        end
      end
      
      @@converters_mutex = Mutex.new
      @@converters = {}

      def row_converter(result)
        @columns = []; translators = []
        result.fields.each_with_index do |f, idx|
          @columns << f.to_sym
          translators << PG_TYPES[result.type(idx)]
        end
        
        # create result signature and memoize the converter
        sig = [@columns, translators].hash
        @@converters_mutex.synchronize do
          @@converters[sig] ||= compile_converter(@columns, translators)
        end
      end
    
      def compile_converter(columns, translators)
        used_columns = []
        kvs = []
        columns.each_with_index do |column, idx|
          next if used_columns.include?(column)
          used_columns << column
        
          if !AUTO_TRANSLATE and translator = translators[idx]
            kvs << ":\"#{column}\" => ((t = r[#{idx}]) ? t.#{translator} : nil)"
          else
            kvs << ":\"#{column}\" => r[#{idx}]"
          end
        end
        eval("lambda {|r| {#{kvs.join(COMMA_SEPARATOR)}}}")
      end

      def array_tuples_fetch_rows(sql, &block)
        @db.synchronize do
          result = @db.execute(sql)
          begin
            conv = array_tuples_row_converter(result)
            result.each {|r| yield conv[r]}
          ensure
            result.clear
          end
        end
      end
      
      @@array_tuples_converters_mutex = Mutex.new
      @@array_tuples_converters = {}

      def array_tuples_row_converter(result)
        @columns = []; translators = []
        result.fields.each_with_index do |f, idx|
          @columns << f.to_sym
          translators << PG_TYPES[result.type(idx)]
        end
        
        # create result signature and memoize the converter
        sig = [@columns, translators].hash
        @@array_tuples_converters_mutex.synchronize do
          @@array_tuples_converters[sig] ||= array_tuples_compile_converter(@columns, translators)
        end
      end
    
      def array_tuples_compile_converter(columns, translators)
        tr = []
        columns.each_with_index do |column, idx|
          if !AUTO_TRANSLATE and t = translators[idx]
            tr << "if (v = r[#{idx}]); r[#{idx}] = v.#{t}; end"
          end
        end
        eval("lambda {|r| r.keys = columns; #{tr.join(';')}; r}")
      end
    end
  end
end
