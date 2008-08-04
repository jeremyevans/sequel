require 'mysql'
require 'sequel_core/adapters/shared/mysql'

# Add methods to get columns, yield hashes with symbol keys, and do
# type conversion.
class Mysql::Result
  # Mapping of type numbers to conversion methods.
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
  
  # Return an array of column name symbols for this result set.
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

  # yield a hash with symbol keys and type converted values.
  def sequel_each_hash(with_table = nil)
    c = columns
    while row = fetch_row
      h = {}
      c.each_with_index {|f, i| h[f] = convert_type(row[i], @column_types[i])}
      yield h
    end
  end
  
  private
  
  # Convert the type of v using the method in MYSQL_TYPES[type].
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
  
end

module Sequel
  # Module for holding all MySQL-related classes and modules for Sequel.
  module MySQL
    # Database class for MySQL databases used with Sequel.
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods
      
      set_adapter_scheme :mysql
      
      # Connect to the database.  In addition to the usual database options,
      # the following options have effect:
      #
      # * :encoding, :charset - Set all the related character sets for this
      #   connection (connection, client, database, server, and results).
      # * :socket - Use a unix socket file instead of connecting via TCP/IP.
      def connect(server)
        opts = server_opts(server)
        conn = Mysql.init
        conn.options(Mysql::OPT_LOCAL_INFILE, "client")
        conn.real_connect(
          opts[:host] || 'localhost',
          opts[:user],
          opts[:password],
          opts[:database],
          opts[:port],
          opts[:socket],
          Mysql::CLIENT_MULTI_RESULTS +
          Mysql::CLIENT_MULTI_STATEMENTS +
          Mysql::CLIENT_COMPRESS
        )
        conn.query_with_result = false
        if encoding = opts[:encoding] || opts[:charset]
          conn.query("set character_set_connection = '#{encoding}'")
          conn.query("set character_set_client = '#{encoding}'")
          conn.query("set character_set_database = '#{encoding}'")
          conn.query("set character_set_server = '#{encoding}'")
          conn.query("set character_set_results = '#{encoding}'")
        end
        conn.meta_eval{attr_accessor :prepared_statements}
        conn.prepared_statements = {}
        conn.reconnect = true
        conn
      end
      
      # Returns instance of Sequel::MySQL::Dataset with the given options.
      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end
      
      # Closes all database connections.
      def disconnect
        @pool.disconnect {|c| c.close}
      end
      
      # Executes the given SQL using an available connection, yielding the
      # connection if the block is given.
      def execute(sql, opts={}, &block)
        return execute_prepared_statement(sql, opts, &block) if Symbol === sql
        begin
          synchronize(opts[:server]){|conn| _execute(conn, sql, opts, &block)}
        rescue Mysql::Error => e
          raise Error.new(e.message)
        end
      end
      
      # Return the version of the MySQL server two which we are connecting.
      def server_version(server=nil)
        @server_version ||= (synchronize(server){|conn| conn.server_version if conn.respond_to?(:server_version)} || super)
      end
      
      # Return an array of symbols specifying table names in the current database.
      def tables(server=nil)
        synchronize(server){|conn| conn.list_tables.map {|t| t.to_sym}}
      end
      
      # Support single level transactions on MySQL.
      def transaction(server=nil)
        synchronize(server) do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
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
      
      # Execute the given SQL on the given connection.  If the :type
      # option is :select, yield the result of the query, otherwise
      # yield the connection if a block is given.
      def _execute(conn, sql, opts)
        log_info(sql)
        conn.query(sql)
        if opts[:type] == :select
          r = conn.use_result
          begin
            yield r
          ensure
            r.free
          end
        else
          yield conn if block_given?
        end
      end
      
      # MySQL doesn't need the connection pool to convert exceptions.
      def connection_pool_default_options
        super.merge(:pool_convert_exceptions=>false)
      end
      
      # The database name when using the native adapter is always stored in
      # the :database option.
      def database_name
        @opts[:database]
      end
      
      # Executes a prepared statement on an available connection.  If the
      # prepared statement already exists for the connection and has the same
      # SQL, reuse it, otherwise, prepare the new statement.  Because of the
      # usual MySQL stupidity, we are forced to name arguments via separate
      # SET queries.  Use @sequel_arg_N (for N starting at 1) for these
      # arguments.
      def execute_prepared_statement(ps_name, opts, &block)
        args = opts[:arguments]
        ps = prepared_statements[ps_name]
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements[ps_name] == sql
            conn.prepared_statements[ps_name] = sql
            s = "PREPARE #{ps_name} FROM '#{::Mysql.quote(sql)}'"
            log_info(s)
            conn.query(s)
          end
          i = 0
          args.each do |arg|
            s = "SET @sequel_arg_#{i+=1} = #{literal(arg)}"
            log_info(s)
            conn.query(s)
          end
          _execute(conn, "EXECUTE #{ps_name}#{" USING #{(1..i).map{|j| "@sequel_arg_#{j}"}.join(', ')}" unless i == 0}", opts, &block)
        end
      end
    end
    
    # Dataset class for MySQL datasets accessed via the native driver.
    class Dataset < Sequel::Dataset
      include Sequel::MySQL::DatasetMethods
      
      # Methods for MySQL prepared statements using the native driver.
      module PreparedStatementMethods
        include Sequel::Dataset::UnnumberedArgumentMapper
        
        # Execute the prepared statement with the bind arguments instead of
        # the given SQL.
        def execute(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
          super(prepared_statement_name, {:arguments=>bind_arguments}.merge(opts), &block)
        end
      end
      
      # Delete rows matching this dataset
      def delete(opts = nil)
        execute_dui(delete_sql(opts)){|c| c.affected_rows}
      end
      
      # Yield all rows matching this dataset
      def fetch_rows(sql)
        execute(sql) do |r|
          @columns = r.columns
          r.sequel_each_hash {|row| yield row}
        end
        self
      end
      
      # Insert a new value into this dataset
      def insert(*values)
        execute_dui(insert_sql(*values)){|c| c.insert_id}
      end
      
      # Handle correct quoting of strings using ::MySQL.quote.
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
      
      # Store the given type of prepared statement in the associated database
      # with the given name.
      def prepare(type, name, values=nil)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        ps.prepared_statement_name = name
        db.prepared_statements[name] = ps
      end
      
      # Replace (update or insert) the matching row.
      def replace(*args)
        execute_dui(replace_sql(*args)){|c| c.insert_id}
      end
      
      # Update the matching rows.
      def update(*args)
        execute_dui(update_sql(*args)){|c| c.affected_rows}
      end
      
      private
      
      # Set the :type option to :select if it hasn't been set.
      def execute(sql, opts={}, &block)
        super(sql, {:type=>:select}.merge(opts), &block)
      end
      
      # Set the :type option to :dui if it hasn't been set.
      def execute_dui(sql, opts={}, &block)
        super(sql, {:type=>:dui}.merge(opts), &block)
      end
    end
  end
end
