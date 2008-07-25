require 'mysql'
require 'sequel_core/adapters/shared/mysql'

# Monkey patch Mysql::Result to yield hashes with symbol keys
class Mysql::Result
  MYSQL_TYPES = {
    0   => :to_d,     # MYSQL_TYPE_DECIMAL
    1   => :to_i,     # MYSQL_TYPE_TINY
    2   => :to_i,     # MYSQL_TYPE_SHORT
    3   => :to_i,     # MYSQL_TYPE_LONG
    4   => :to_f,     # MYSQL_TYPE_FLOAT
    5   => :to_f,     # MYSQL_TYPE_DOUBLE
    # 6   => ??,        # MYSQL_TYPE_NULL
    7   => :to_sequel_time,  # MYSQL_TYPE_TIMESTAMP
    8   => :to_i,     # MYSQL_TYPE_LONGLONG
    9   => :to_i,     # MYSQL_TYPE_INT24
    10  => :to_date,  # MYSQL_TYPE_DATE
    11  => :to_time,  # MYSQL_TYPE_TIME
    12  => :to_sequel_time,  # MYSQL_TYPE_DATETIME
    13  => :to_i,     # MYSQL_TYPE_YEAR
    14  => :to_date,  # MYSQL_TYPE_NEWDATE
    # 15  => :to_s      # MYSQL_TYPE_VARCHAR
    # 16  => :to_s,     # MYSQL_TYPE_BIT
    246 => :to_d,     # MYSQL_TYPE_NEWDECIMAL
    247 => :to_i,     # MYSQL_TYPE_ENUM
    248 => :to_i,      # MYSQL_TYPE_SET
    249 => :to_blob,     # MYSQL_TYPE_TINY_BLOB
    250 => :to_blob,     # MYSQL_TYPE_MEDIUM_BLOB
    251 => :to_blob,     # MYSQL_TYPE_LONG_BLOB
    252 => :to_blob,     # MYSQL_TYPE_BLOB
    # 253 => :to_s,     # MYSQL_TYPE_VAR_STRING
    # 254 => :to_s,     # MYSQL_TYPE_STRING
    # 255 => :to_s      # MYSQL_TYPE_GEOMETRY
  }

  def convert_type(v, type)
    if v
      if type == 1 && Sequel.convert_tinyint_to_bool
        # We special case tinyint here to avoid adding
        # a method to an ancestor of Fixnum
        v.to_i == 0 ? false : true
      else
        (t = MYSQL_TYPES[type]) ? v.send(t) : v
      end
    else
      nil
    end
  end

  def columns(with_table = nil)
    unless @columns
      @column_types = []
      @columns = fetch_fields.map do |f|
        @column_types << f.type
        (with_table ? "#{f.table}.#{f.name}" : f.name).to_sym
      end
    end
    @columns
  end

  def each_array(with_table = nil)
    c = columns
    while row = fetch_row
      c.each_with_index do |f, i|
        if (t = MYSQL_TYPES[@column_types[i]]) && (v = row[i])
          row[i] = v.send(t)
        end
      end
      yield row
    end
  end

  def sequel_each_hash(with_table = nil)
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
      include Sequel::MySQL::DatabaseMethods
      
      set_adapter_scheme :mysql

      def connect
        conn = Mysql.init
        conn.options(Mysql::OPT_LOCAL_INFILE, "client")
        conn.real_connect(
          @opts[:host] || 'localhost',
          @opts[:user],
          @opts[:password],
          @opts[:database],
          @opts[:port],
          @opts[:socket],
          Mysql::CLIENT_MULTI_RESULTS +
          Mysql::CLIENT_MULTI_STATEMENTS +
          Mysql::CLIENT_COMPRESS
        )
        conn.query_with_result = false
        if encoding = @opts[:encoding] || @opts[:charset]
          conn.query("set character_set_connection = '#{encoding}'")
          conn.query("set character_set_client = '#{encoding}'")
          conn.query("set character_set_database = '#{encoding}'")
          conn.query("set character_set_server = '#{encoding}'")
          conn.query("set character_set_results = '#{encoding}'")
        end
        conn.reconnect = true
        conn
      end
      
      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end

      def disconnect
        @pool.disconnect {|c| c.close}
      end

      def execute(sql, &block)
        begin
          log_info(sql)
          @pool.hold do |conn|
            conn.query(sql)
            block[conn] if block
          end
        rescue Mysql::Error => e
          raise Error.new(e.message)
        end
      end

      def execute_select(sql, &block)
        execute(sql) do |c|
          r = c.use_result
          begin
            block[r]
          ensure
            r.free
          end
        end
      end
      
      def server_version
        @server_version ||= (synchronize{|conn| conn.server_version if conn.respond_to?(:server_version)} || super)
      end
      
      def tables
        @pool.hold do |conn|
          conn.list_tables.map {|t| t.to_sym}
        end
      end
      
      def transaction
        @pool.hold do |conn|
          @transactions ||= []
          return yield(conn) if @transactions.include? Thread.current
          log_info(SQL_BEGIN)
          conn.query(SQL_BEGIN)
          begin
            @transactions << Thread.current
            yield(conn)
          rescue ::Exception => e
            log_info(SQL_ROLLBACK)
            conn.query(SQL_ROLLBACK)
            raise (Mysql::Error === e ? Error.new(e.message) : e) unless Error::Rollback === e
          ensure
            unless e
              log_info(SQL_COMMIT)
              conn.query(SQL_COMMIT)
            end
            @transactions.delete(Thread.current)
          end
        end
      end

      private
      
      def connection_pool_default_options
        super.merge(:pool_convert_exceptions=>false)
      end
      
      def database_name
        @opts[:database]
      end
    end

    class Dataset < Sequel::Dataset
      include Sequel::MySQL::DatasetMethods

      def delete(opts = nil)
        @db.execute(delete_sql(opts)) {|c| c.affected_rows}
      end

      def fetch_rows(sql)
        @db.execute_select(sql) do |r|
          @columns = r.columns
          r.sequel_each_hash {|row| yield row}
        end
        self
      end

      def insert(*values)
        @db.execute(insert_sql(*values)) {|c| c.insert_id}
      end
      
      def literal(v)
        case v
        when LiteralString
          v
        when String
          "'#{::Mysql.quote(v)}'"
        else
          super
        end
      end
      
      def replace(*args)
        @db.execute(replace_sql(*args)) {|c| c.insert_id}
      end
      
      def update(*args)
        @db.execute(update_sql(*args)) {|c| c.affected_rows}
      end
    end
  end
end
