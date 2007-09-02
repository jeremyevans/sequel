if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

require 'mysql'

# Monkey patch Mysql::Result to yield hashes with symbol keys
class Mysql::Result
  MYSQL_TYPES = {
    0 => :to_i,
    1 => :to_i,
    2 => :to_i,
    3 => :to_i,
    4 => :to_f,
    5 => :to_f,
    7 => :to_time,
    8 => :to_i,
    9 => :to_i,
    10 => :to_time,
    11 => :to_time,
    12 => :to_time,
    13 => :to_i,
    14 => :to_time,
    247 => :to_i,
    248 => :to_i
  }
  
  def convert_type(v, type)
    v ? ((t = MYSQL_TYPES[type]) ? v.send(t) : v) : nil
  end
  
  def columns(with_table = nil)
    unless @columns
      @column_types = []
      @columns = fetch_fields.map do |f|
        @column_types << f.type
        (with_table ? (f.table + "." + f.name) : f.name).to_sym
      end
    end
    @columns
  end
  
  def each_hash(with_table=nil)
    c = columns
    while row = fetch_row
      h = {}
      c.each_with_index {|f, i| h[f] = convert_type(row[i], @column_types[i])}
      yield h
    end
  end
end

module Sequel
  module MySQL
    class Database < Sequel::Database
      set_adapter_scheme :mysql
    
      def serial_primary_key_options
        {:primary_key => true, :type => :integer, :auto_increment => true}
      end
      
      AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze
      
      def auto_increment_sql
        AUTO_INCREMENT
      end

      def connect
        conn = Mysql.real_connect(@opts[:host], @opts[:user], @opts[:password], 
          @opts[:database], @opts[:port])
        conn.query_with_result = false
        conn.reconnect = true
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
      UNQUOTABLE_FIELD_RE = /^(`(.+)`)|\*$/.freeze
      def quote_field(f)
        f =~ UNQUOTABLE_FIELD_RE ? f : "`#{f}`"
      end
      
      FIELD_EXPR_RE = /^([^\(]+\()?([^\.]+\.)?([^\s\)]+)(\))?(\sAS\s(.+))?$/i.freeze
      FIELD_ORDER_RE = /^(.*) (DESC|ASC)$/i.freeze
      def quoted_field_name(name)
        case name
        when FIELD_EXPR_RE:
          $6 ? \
            "#{$1}#{$2}#{quote_field($3)}#{$4} AS #{quote_field($6)}" : \
            "#{$1}#{$2}#{quote_field($3)}#{$4}"
        when FIELD_ORDER_RE: "#{quote_field($1)} #{$2}"
        else
          quote_field(name)
        end
      end
      
      TRUE = '1'
      FALSE = '0'
      
      def literal(v)
        case v
        when true: TRUE
        when false: FALSE
        else
          super
        end
      end
      
      def match_expr(l, r)
        case r
        when Regexp:
          "(#{literal(l)} REGEXP #{literal(r.source)})"
        else
          super
        end
      end
      
      # MySQL supports ORDER and LIMIT clauses in UPDATE statements.
      def update_sql(values, opts = nil)
        sql = super

        opts = opts ? @opts.merge(opts) : @opts
        
        if order = opts[:order]
          sql << " ORDER BY #{field_list(order)}"
        end

        if limit = opts[:limit]
          sql << " LIMIT #{limit}"
        end

        sql
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
      
      def fetch_rows(sql)
        @db.synchronize do
          r = @db.query(sql)
          begin
            @columns = r.columns
            r.each_hash {|row| yield row}
          ensure
            r.free
          end
        end
        self
      end
    end
  end
end