require 'mysql'
require 'sequel_core/adapters/shared/mysql'
require 'sequel_core/adapters/utils/stored_procedures'

module Sequel
  # Module for holding all MySQL-related classes and modules for Sequel.
  module MySQL
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
      249 => :to_sequel_blob,     # MYSQL_TYPE_TINY_BLOB
      250 => :to_sequel_blob,     # MYSQL_TYPE_MEDIUM_BLOB
      251 => :to_sequel_blob,     # MYSQL_TYPE_LONG_BLOB
      252 => :to_sequel_blob,     # MYSQL_TYPE_BLOB
      # 253 => :to_s,     # MYSQL_TYPE_VAR_STRING
      # 254 => :to_s,     # MYSQL_TYPE_STRING
      # 255 => :to_s      # MYSQL_TYPE_GEOMETRY
    }
  
    # Database class for MySQL databases used with Sequel.
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods
      
      set_adapter_scheme :mysql
      
      # Support stored procedures on MySQL
      def call_sproc(name, opts={}, &block)
        args = opts[:args] || [] 
        execute("CALL #{name}#{args.empty? ? '()' : literal(args)}", opts.merge(:sproc=>false), &block)
      end
      
      # Connect to the database.  In addition to the usual database options,
      # the following options have effect:
      #
      # * :auto_is_null - Set to true to use MySQL default behavior of having
      #   a filter for an autoincrement column equals NULL to return the last
      #   inserted row.
      # * :charset - Same as :encoding (:encoding takes precendence)
      # * :compress - Set to false to not compress results from the server
      # * :encoding - Set all the related character sets for this
      #   connection (connection, client, database, server, and results).
      # * :socket - Use a unix socket file instead of connecting via TCP/IP.
      # * :timeout - Set the timeout in seconds before the server will
      #   disconnect this connection.
      def connect(server)
        opts = server_opts(server)
        conn = Mysql.init
        conn.options(Mysql::OPT_LOCAL_INFILE, "client")
        if encoding = opts[:encoding] || opts[:charset]
          # set charset _before_ the connect. using an option instead of "SET (NAMES|CHARACTER_SET_*)" works across reconnects
          conn.options(Mysql::SET_CHARSET_NAME, encoding)
        end
        conn.real_connect(
          opts[:host] || 'localhost',
          opts[:user],
          opts[:password],
          opts[:database],
          opts[:port],
          opts[:socket],
          Mysql::CLIENT_MULTI_RESULTS +
          Mysql::CLIENT_MULTI_STATEMENTS +
          (opts[:compress] == false ? 0 : Mysql::CLIENT_COMPRESS)
        )

        # increase timeout so mysql server doesn't disconnect us
        conn.query("set @@wait_timeout = #{opts[:timeout] || 2592000}")

        # By default, MySQL 'where id is null' selects the last inserted id
        conn.query("set SQL_AUTO_IS_NULL=0") unless opts[:auto_is_null]

        conn.query_with_result = false
        conn.meta_eval{attr_accessor :prepared_statements}
        conn.prepared_statements = {}
        conn.reconnect = true
        conn
      end
      
      # Returns instance of Sequel::MySQL::Dataset with the given options.
      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end
      
      # Executes the given SQL using an available connection, yielding the
      # connection if the block is given.
      def execute(sql, opts={}, &block)
        return call_sproc(sql, opts, &block) if opts[:sproc]
        return execute_prepared_statement(sql, opts, &block) if Symbol === sql
        begin
          synchronize(opts[:server]){|conn| _execute(conn, sql, opts, &block)}
        rescue Mysql::Error => e
          raise_error(e)
        end
      end
      
      # Return the version of the MySQL server two which we are connecting.
      def server_version(server=nil)
        @server_version ||= (synchronize(server){|conn| conn.server_version if conn.respond_to?(:server_version)} || super)
      end
      
      # Support single level transactions on MySQL.
      def transaction(server=nil)
        synchronize(server) do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
          log_info(begin_transaction_sql)
          conn.query(begin_transaction_sql)
          begin
            @transactions << Thread.current
            yield(conn)
          rescue ::Exception => e
            log_info(rollback_transaction_sql)
            conn.query(rollback_transaction_sql)
            transaction_error(e, Mysql::Error)
          ensure
            unless e
              log_info(commit_transaction_sql)
              conn.query(commit_transaction_sql)
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
          loop do
            begin
              r = conn.use_result
            rescue Mysql::Error
              nil
            else
              begin
                yield r
              ensure
                r.free
              end
            end
            break unless conn.respond_to?(:next_result) && conn.next_result
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
      
      # Closes given database connection.
      def disconnect_connection(c)
        c.close
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
      include StoredProcedures
      
      # Methods to add to MySQL prepared statement calls without using a
      # real database prepared statement and bound variables.
      module CallableStatementMethods
        # Extend given dataset with this module so subselects inside subselects in
        # prepared statements work.
        def subselect_sql(ds)
          ps = ds.to_prepared_statement(:select)
          ps.extend(CallableStatementMethods)
          ps.prepared_args = prepared_args
          ps.prepared_sql
        end
      end
      
      # Methods for MySQL prepared statements using the native driver.
      module PreparedStatementMethods
        include Sequel::Dataset::UnnumberedArgumentMapper
        
        private
        
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
      
      # Methods for MySQL stored procedures using the native driver.
      module StoredProcedureMethods
        include Sequel::Dataset::StoredProcedureMethods
        
        private
        
        # Execute the database stored procedure with the stored arguments.
        def execute(sql, opts={}, &block)
          super(@sproc_name, {:args=>@sproc_args, :sproc=>true}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
          super(@sproc_name, {:args=>@sproc_args, :sproc=>true}.merge(opts), &block)
        end
      end
      
      # MySQL is different in that it supports prepared statements but not bound
      # variables outside of prepared statements.  The default implementation
      # breaks the use of subselects in prepared statements, so extend the
      # temporary prepared statement that this creates with a module that
      # fixes it.
      def call(type, bind_arguments={}, values=nil)
        ps = to_prepared_statement(type, values)
        ps.extend(CallableStatementMethods)
        ps.call(bind_arguments)
      end
      
      # Delete rows matching this dataset
      def delete(opts = nil)
        execute_dui(delete_sql(opts)){|c| c.affected_rows}
      end
      
      # Yield all rows matching this dataset
      def fetch_rows(sql)
        execute(sql) do |r|
          column_types = []
          @columns = r.fetch_fields.map{|f| column_types << f.type; output_identifier(f.name)}
          while row = r.fetch_row
            h = {}
            @columns.each_with_index {|f, i| h[f] = convert_type(row[i], column_types[i])}
            yield h
          end
        end
        self
      end
      
      # Insert a new value into this dataset
      def insert(*values)
        execute_dui(insert_sql(*values)){|c| c.insert_id}
      end
      
      # Store the given type of prepared statement in the associated database
      # with the given name.
      def prepare(type, name=nil, values=nil)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        if name
          ps.prepared_statement_name = name
          db.prepared_statements[name] = ps
        end
        ps
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
      
      # Set the :type option to :select if it hasn't been set.
      def execute(sql, opts={}, &block)
        super(sql, {:type=>:select}.merge(opts), &block)
      end
      
      # Set the :type option to :dui if it hasn't been set.
      def execute_dui(sql, opts={}, &block)
        super(sql, {:type=>:dui}.merge(opts), &block)
      end
      
      # Handle correct quoting of strings using ::MySQL.quote.
      def literal_string(v)
        "'#{::Mysql.quote(v)}'"
      end
      
      # Extend the dataset with the MySQL stored procedure methods.
      def prepare_extend_sproc(ds)
        ds.extend(StoredProcedureMethods)
      end
    end
  end
end
