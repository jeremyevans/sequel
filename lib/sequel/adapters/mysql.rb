begin
  require "mysqlplus"
rescue LoadError
  require 'mysql'
end
raise(LoadError, "require 'mysql' did not define Mysql::CLIENT_MULTI_RESULTS!\n  You are probably using the pure ruby mysql.rb driver,\n  which Sequel does not support. You need to install\n  the C based adapter, and make sure that the mysql.so\n  file is loaded instead of the mysql.rb file.\n") unless defined?(Mysql::CLIENT_MULTI_RESULTS)

Sequel.require %w'shared/mysql utils/stored_procedures', 'adapters'

module Sequel
  # Module for holding all MySQL-related classes and modules for Sequel.
  module MySQL
    TYPE_TRANSLATOR = tt = Class.new do
      def boolean(s) s.to_i != 0 end
      def blob(s) ::Sequel::SQL::Blob.new(s) end
      def integer(s) s.to_i end
      def float(s) s.to_f end
      def decimal(s) ::BigDecimal.new(s) end
      def date(s) ::Sequel.string_to_date(s) end
      def time(s) ::Sequel.string_to_time(s) end
      def timestamp(s) ::Sequel.database_to_application_timestamp(s) end
      def date_conv(s) ::Sequel::MySQL.convert_date_time(:string_to_date, s) end
      def time_conv(s) ::Sequel::MySQL.convert_date_time(:string_to_time, s) end
      def timestamp_conv(s) ::Sequel::MySQL.convert_date_time(:database_to_application_timestamp, s) end
    end.new

    # Hash with integer keys and callable values for converting MySQL types.
    MYSQL_TYPES = {}
    {
      [0, 246]  => tt.method(:decimal),
      [2, 3, 8, 9, 13, 247, 248]  => tt.method(:integer),
      [4, 5]  => tt.method(:float),
      [249, 250, 251, 252]  => tt.method(:blob)
    }.each do |k,v|
      k.each{|n| MYSQL_TYPES[n] = v}
    end

    # Modify the type translator used for the tinyint type based
    # on the value given.
    def self.convert_tinyint_to_bool=(v)
      MYSQL_TYPES[1] = TYPE_TRANSLATOR.method(v ? :boolean : :integer)
      @convert_tinyint_to_bool = v
    end
    self.convert_tinyint_to_bool = convert_tinyint_to_bool

    class << self
      # By default, Sequel raises an exception if in invalid date or time is used.
      # However, if this is set to nil or :nil, the adapter treats dates
      # like 0000-00-00 and times like 838:00:00 as nil values.  If set to :string,
      # it returns the strings as is.  
      attr_reader :convert_invalid_date_time
    end

    # Modify the type translators for the date, time, and timestamp types
    # depending on the value given.
    def self.convert_invalid_date_time=(v)
      MYSQL_TYPES[11] = TYPE_TRANSLATOR.method(v == false ? :time : :time_conv)
      m = TYPE_TRANSLATOR.method(v == false ? :date : :date_conv)
      [10, 14].each{|i| MYSQL_TYPES[i] = m}
      m = TYPE_TRANSLATOR.method(v == false ? :timestamp : :timestamp_conv)
      [7, 12].each{|i| MYSQL_TYPES[i] = m}
      @convert_invalid_date_time = v
    end
    self.convert_invalid_date_time = false

    # If convert_invalid_date_time is nil, :nil, or :string and
    # the conversion raises an InvalidValue exception, return v
    # if :string and nil otherwise.
    def self.convert_date_time(meth, v)
      begin
        Sequel.send(meth, v)
      rescue InvalidValue
        case @convert_invalid_date_time
        when nil, :nil
          nil
        when :string
          v
        else 
          raise
        end
      end
    end
  
    # Database class for MySQL databases used with Sequel.
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods
      
      # Mysql::Error messages that indicate the current connection should be disconnected
      MYSQL_DATABASE_DISCONNECT_ERRORS = /\A(Commands out of sync; you can't run this command now|Can't connect to local MySQL server through socket|MySQL server has gone away)/
      
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
      # * :config_default_group - The default group to read from the in
      #   the MySQL config file.
      # * :config_local_infile - If provided, sets the Mysql::OPT_LOCAL_INFILE
      #   option on the connection with the given value.
      # * :connect_timeout - Set the timeout in seconds before a connection
      #   attempt is abandoned.
      # * :encoding - Set all the related character sets for this
      #   connection (connection, client, database, server, and results).
      # * :read_timeout - Set the timeout in seconds for reading back results
      #   to a query.
      # * :socket - Use a unix socket file instead of connecting via TCP/IP.
      # * :timeout - Set the timeout in seconds before the server will
      #   disconnect this connection (a.k.a @@wait_timeout).
      def connect(server)
        opts = server_opts(server)
        conn = Mysql.init
        conn.options(Mysql::READ_DEFAULT_GROUP, opts[:config_default_group] || "client")
        conn.options(Mysql::OPT_LOCAL_INFILE, opts[:config_local_infile]) if opts.has_key?(:config_local_infile)
        conn.ssl_set(opts[:sslkey], opts[:sslcert], opts[:sslca], opts[:sslcapath], opts[:sslcipher]) if opts[:sslca] || opts[:sslkey]
        if encoding = opts[:encoding] || opts[:charset]
          # Set encoding before connecting so that the mysql driver knows what
          # encoding we want to use, but this can be overridden by READ_DEFAULT_GROUP.
          conn.options(Mysql::SET_CHARSET_NAME, encoding)
        end
        if read_timeout = opts[:read_timeout] and defined? Mysql::OPT_READ_TIMEOUT
          conn.options(Mysql::OPT_READ_TIMEOUT, read_timeout)
        end
        if connect_timeout = opts[:connect_timeout] and defined? Mysql::OPT_CONNECT_TIMEOUT
          conn.options(Mysql::OPT_CONNECT_TIMEOUT, connect_timeout)
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
        sqls = []
        # Set encoding a slightly different way after connecting,
        # in case the READ_DEFAULT_GROUP overrode the provided encoding.
        # Doesn't work across implicit reconnects, but Sequel doesn't turn on
        # that feature.
        sqls << "SET NAMES #{literal(encoding.to_s)}" if encoding

        # Increase timeout so mysql server doesn't disconnect us
        # Value used by default is maximum allowed value on Windows.
        sqls << "SET @@wait_timeout = #{opts[:timeout] || 2147483}"

        # By default, MySQL 'where id is null' selects the last inserted id
        sqls << "SET SQL_AUTO_IS_NULL=0" unless opts[:auto_is_null]

        sqls.each{|sql| log_yield(sql){conn.query(sql)}}

        class << conn
          attr_accessor :prepared_statements
        end
        conn.prepared_statements = {}
        conn
      end
      
      # Returns instance of Sequel::MySQL::Dataset with the given options.
      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end
      
      # Executes the given SQL using an available connection, yielding the
      # connection if the block is given.
      def execute(sql, opts={}, &block)
        if opts[:sproc]
          call_sproc(sql, opts, &block)
        elsif sql.is_a?(Symbol)
          execute_prepared_statement(sql, opts, &block)
        else
          synchronize(opts[:server]){|conn| _execute(conn, sql, opts, &block)}
        end
      end
      
      # Return the version of the MySQL server two which we are connecting.
      def server_version(server=nil)
        @server_version ||= (synchronize(server){|conn| conn.server_version if conn.respond_to?(:server_version)} || super)
      end

      private
      
      # Execute the given SQL on the given connection.  If the :type
      # option is :select, yield the result of the query, otherwise
      # yield the connection if a block is given.
      def _execute(conn, sql, opts)
        begin
          r = log_yield(sql){conn.query(sql)}
          if opts[:type] == :select
            yield r if r
          elsif block_given?
            yield conn
          end
          if conn.respond_to?(:more_results?)
            while conn.more_results? do
              if r
                r.free
                r = nil
              end
              begin
                conn.next_result
                r = conn.use_result
              rescue Mysql::Error => e
                raise_error(e, :disconnect=>true) if MYSQL_DATABASE_DISCONNECT_ERRORS.match(e.message)
                break
              end
              yield r if opts[:type] == :select
            end
          end
        rescue Mysql::Error => e
          raise_error(e, :disconnect=>MYSQL_DATABASE_DISCONNECT_ERRORS.match(e.message))
        ensure
          r.free if r
          # Use up all results to avoid a commands out of sync message.
          if conn.respond_to?(:more_results?)
            while conn.more_results? do
              begin
                conn.next_result
                r = conn.use_result
              rescue Mysql::Error => e
                raise_error(e, :disconnect=>true) if MYSQL_DATABASE_DISCONNECT_ERRORS.match(e.message)
                break
              end
              r.free if r
            end
          end
        end
      end
      
      # MySQL connections use the query method to execute SQL without a result
      def connection_execute_method
        :query
      end
      
      # The MySQL adapter main error class is Mysql::Error
      def database_error_classes
        [Mysql::Error]
      end
      
      # The database name when using the native adapter is always stored in
      # the :database option.
      def database_name
        @opts[:database]
      end
      
      # Closes given database connection.
      def disconnect_connection(c)
        c.close
      rescue Mysql::Error
        nil
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
            _execute(conn, "PREPARE #{ps_name} FROM '#{::Mysql.quote(sql)}'", opts)
          end
          i = 0
          _execute(conn, "SET " + args.map {|arg| "@sequel_arg_#{i+=1} = #{literal(arg)}"}.join(", "), opts) unless args.empty?
          _execute(conn, "EXECUTE #{ps_name}#{" USING #{(1..i).map{|j| "@sequel_arg_#{j}"}.join(', ')}" unless i == 0}", opts, &block)
        end
      end
      
      # Convert tinyint(1) type to boolean if convert_tinyint_to_bool is true
      def schema_column_type(db_type)
        Sequel::MySQL.convert_tinyint_to_bool && db_type == 'tinyint(1)' ? :boolean : super
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
          ps = ps.bind(@opts[:bind_vars]) if @opts[:bind_vars]
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
      def call(type, bind_arguments={}, *values, &block)
        ps = to_prepared_statement(type, values)
        ps.extend(CallableStatementMethods)
        ps.call(bind_arguments, &block)
      end
      
      # Delete rows matching this dataset
      def delete
        execute_dui(delete_sql){|c| return c.affected_rows}
      end
      
      # Yield all rows matching this dataset.  If the dataset is set to
      # split multiple statements, yield arrays of hashes one per statement
      # instead of yielding results for all statements as hashes.
      def fetch_rows(sql, &block)
        execute(sql) do |r|
          i = -1
          cols = r.fetch_fields.map do |f| 
            # Pretend tinyint is another integer type if its length is not 1, to
            # avoid casting to boolean if Sequel::MySQL.convert_tinyint_to_bool
            # is set.
            type_proc = f.type == 1 && f.length != 1 ? MYSQL_TYPES[2] : MYSQL_TYPES[f.type]
            [output_identifier(f.name), type_proc, i+=1]
          end
          @columns = cols.map{|c| c.first}
          if opts[:split_multiple_result_sets]
            s = []
            yield_rows(r, cols){|h| s << h}
            yield s
          else
            yield_rows(r, cols, &block)
          end
        end
        self
      end
      
      # Don't allow graphing a dataset that splits multiple statements
      def graph(*)
        raise(Error, "Can't graph a dataset that splits multiple result sets") if opts[:split_multiple_result_sets]
        super
      end
      
      # Insert a new value into this dataset
      def insert(*values)
        execute_dui(insert_sql(*values)){|c| return c.insert_id}
      end
      
      # Store the given type of prepared statement in the associated database
      # with the given name.
      def prepare(type, name=nil, *values)
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
        execute_dui(replace_sql(*args)){|c| return c.insert_id}
      end
      
      # Makes each yield arrays of rows, with each array containing the rows
      # for a given result set.  Does not work with graphing.  So you can submit
      # SQL with multiple statements and easily determine which statement
      # returned which results.
      #
      # Modifies the row_proc of the returned dataset so that it still works
      # as expected (running on the hashes instead of on the arrays of hashes).
      # If you modify the row_proc afterward, note that it will receive an array
      # of hashes instead of a hash.
      def split_multiple_result_sets
        raise(Error, "Can't split multiple statements on a graphed dataset") if opts[:graph]
        ds = clone(:split_multiple_result_sets=>true)
        ds.row_proc = proc{|x| x.map{|h| row_proc.call(h)}} if row_proc
        ds
      end
      
      # Update the matching rows.
      def update(values={})
        execute_dui(update_sql(values)){|c| return c.affected_rows}
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
      
      # Handle correct quoting of strings using ::MySQL.quote.
      def literal_string(v)
        "'#{::Mysql.quote(v)}'"
      end
      
      # Extend the dataset with the MySQL stored procedure methods.
      def prepare_extend_sproc(ds)
        ds.extend(StoredProcedureMethods)
      end
      
      # Yield each row of the given result set r with columns cols
      # as a hash with symbol keys
      def yield_rows(r, cols)
        while row = r.fetch_row
          h = {}
          cols.each{|n, p, i| v = row[i]; h[n] = (v && p) ? p.call(v) : v}
          yield h
        end
      end
    end
  end
end
