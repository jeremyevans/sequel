module Sequel
  # Hash of adapters that have been used. The key is the adapter scheme
  # symbol, and the value is the Database subclass.
  ADAPTER_MAP = {}
    
  # Array of all databases to which Sequel has connected.  If you are
  # developing an application that can connect to an arbitrary number of 
  # databases, delete the database objects from this or they will not get
  # garbage collected.
  DATABASES = []

  # A Database object represents a virtual connection to a database.
  # The Database class is meant to be subclassed by database adapters in order
  # to provide the functionality needed for executing queries.
  class Database
    extend Metaprogramming
    include Metaprogramming

    # Array of supported database adapters
    ADAPTERS = %w'ado amalgalite db2 dbi do firebird informix jdbc mysql odbc openbase oracle postgres sqlite'.collect{|x| x.to_sym}

    SQL_BEGIN = 'BEGIN'.freeze
    SQL_COMMIT = 'COMMIT'.freeze
    SQL_RELEASE_SAVEPOINT = 'RELEASE SAVEPOINT autopoint_%d'.freeze
    SQL_ROLLBACK = 'ROLLBACK'.freeze
    SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze
    SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
    
    TRANSACTION_BEGIN = 'Transaction.begin'.freeze
    TRANSACTION_COMMIT = 'Transaction.commit'.freeze
    TRANSACTION_ROLLBACK = 'Transaction.rollback'.freeze
    
    POSTGRES_DEFAULT_RE = /\A(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))\z/
    MSSQL_DEFAULT_RE = /\A(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))\z/
    MYSQL_TIMESTAMP_RE = /\ACURRENT_(?:DATE|TIMESTAMP)?\z/
    STRING_DEFAULT_RE = /\A'(.*)'\z/

    # The identifier input method to use by default
    @@identifier_input_method = nil

    # The identifier output method to use by default
    @@identifier_output_method = nil

    # Whether to use the single threaded connection pool by default
    @@single_threaded = false

    # Whether to quote identifiers (columns and tables) by default
    @@quote_identifiers = nil

    # The default schema to use, generally should be nil.
    attr_accessor :default_schema

    # Array of SQL loggers to use for this database
    attr_accessor :loggers
    
    # The options for this database
    attr_reader :opts
    
    # The connection pool for this database
    attr_reader :pool

    # The prepared statement objects for this database, keyed by name
    attr_reader :prepared_statements
    
    # Constructs a new instance of a database connection with the specified
    # options hash.
    #
    # Sequel::Database is an abstract class that is not useful by itself.
    #
    # Takes the following options:
    # * :default_schema : The default schema to use, should generally be nil
    # * :disconnection_proc: A proc used to disconnect the connection.
    # * :identifier_input_method: A string method symbol to call on identifiers going into the database
    # * :identifier_output_method: A string method symbol to call on identifiers coming from the database
    # * :loggers : An array of loggers to use.
    # * :quote_identifiers : Whether to quote identifiers
    # * :single_threaded : Whether to use a single-threaded connection pool
    #
    # All options given are also passed to the ConnectionPool.  If a block
    # is given, it is used as the connection_proc for the ConnectionPool.
    def initialize(opts = {}, &block)
      @opts ||= opts
      
      @single_threaded = opts.include?(:single_threaded) ? typecast_value_boolean(opts[:single_threaded]) : @@single_threaded
      @schemas = {}
      @default_schema = opts.include?(:default_schema) ? opts[:default_schema] : default_schema_default
      @prepared_statements = {}
      @transactions = []
      @identifier_input_method = nil
      @identifier_output_method = nil
      @quote_identifiers = nil
      @pool = (@single_threaded ? SingleThreadedPool : ConnectionPool).new(connection_pool_default_options.merge(opts), &block)
      @pool.connection_proc = proc{|server| connect(server)} unless block
      @pool.disconnection_proc = proc{|conn| disconnect_connection(conn)} unless opts[:disconnection_proc]

      @loggers = Array(opts[:logger]) + Array(opts[:loggers])
      ::Sequel::DATABASES.push(self)
    end
    
    ### Class Methods ###

    # The Database subclass for the given adapter scheme.
    # Raises Sequel::AdapterNotFound if the adapter
    # could not be loaded.
    def self.adapter_class(scheme)
      scheme = scheme.to_s.gsub('-', '_').to_sym
      
      unless klass = ADAPTER_MAP[scheme]
        # attempt to load the adapter file
        begin
          Sequel.require "adapters/#{scheme}"
        rescue LoadError => e
          raise Sequel.convert_exception_class(e, AdapterNotFound)
        end
        
        # make sure we actually loaded the adapter
        unless klass = ADAPTER_MAP[scheme]
          raise AdapterNotFound, "Could not load #{scheme} adapter"
        end
      end
      klass
    end
        
    # Returns the scheme for the Database class.
    def self.adapter_scheme
      @scheme
    end
    
    # Connects to a database.  See Sequel.connect.
    def self.connect(conn_string, opts = {}, &block)
      case conn_string
      when String
        if match = /\A(jdbc|do):/o.match(conn_string)
          c = adapter_class(match[1].to_sym)
          opts = {:uri=>conn_string}.merge(opts)
        else
          uri = URI.parse(conn_string)
          scheme = uri.scheme
          scheme = :dbi if scheme =~ /\Adbi-/
          c = adapter_class(scheme)
          uri_options = c.send(:uri_to_options, uri)
          uri.query.split('&').collect{|s| s.split('=')}.each{|k,v| uri_options[k.to_sym] = v} unless uri.query.to_s.strip.empty?
          uri_options.entries.each{|k,v| uri_options[k] = URI.unescape(v) if v.is_a?(String)}
          opts = uri_options.merge(opts)
        end
      when Hash
        opts = conn_string.merge(opts)
        c = adapter_class(opts[:adapter] || opts['adapter'])
      else
        raise Error, "Sequel::Database.connect takes either a Hash or a String, given: #{conn_string.inspect}"
      end
      # process opts a bit
      opts = opts.inject({}) do |m, kv| k, v = *kv
        k = :user if k.to_s == 'username'
        m[k.to_sym] = v
        m
      end
      if block
        begin
          yield(db = c.new(opts))
        ensure
          db.disconnect if db
          ::Sequel::DATABASES.delete(db)
        end
        nil
      else
        c.new(opts)
      end
    end
    
    # The method to call on identifiers going into the database
    def self.identifier_input_method
      @@identifier_input_method
    end
    
    # Set the method to call on identifiers going into the database
    # See Sequel.identifier_input_method=.
    def self.identifier_input_method=(v)
      @@identifier_input_method = v || ""
    end
    
    # The method to call on identifiers coming from the database
    def self.identifier_output_method
      @@identifier_output_method
    end
    
    # Set the method to call on identifiers coming from the database
    # See Sequel.identifier_output_method=.
    def self.identifier_output_method=(v)
      @@identifier_output_method = v || ""
    end

    # Sets the default quote_identifiers mode for new databases.
    # See Sequel.quote_identifiers=.
    def self.quote_identifiers=(value)
      @@quote_identifiers = value
    end

    # Sets the default single_threaded mode for new databases.
    # See Sequel.single_threaded=.
    def self.single_threaded=(value)
      @@single_threaded = value
    end

    ### Private Class Methods ###

    # Sets the adapter scheme for the Database class. Call this method in
    # descendants of Database to allow connection using a URL. For example the
    # following:
    #
    #   class Sequel::MyDB::Database < Sequel::Database
    #     set_adapter_scheme :mydb
    #     ...
    #   end
    #
    # would allow connection using:
    #
    #   Sequel.connect('mydb://user:password@dbserver/mydb')
    def self.set_adapter_scheme(scheme) # :nodoc:
      @scheme = scheme
      ADAPTER_MAP[scheme.to_sym] = self
    end
    
    # Converts a uri to an options hash. These options are then passed
    # to a newly created database object. 
    def self.uri_to_options(uri) # :nodoc:
      { :user => uri.user,
        :password => uri.password,
        :host => uri.host,
        :port => uri.port,
        :database => (m = /\/(.*)/.match(uri.path)) && (m[1]) }
    end
    
    private_class_method :set_adapter_scheme, :uri_to_options
    
    ### Instance Methods ###

    # Runs the supplied SQL statement string on the database server.
    # Alias for run.
    def <<(sql)
      run(sql)
    end
    
    # Returns a dataset from the database. If the first argument is a string,
    # the method acts as an alias for Database#fetch, returning a dataset for
    # arbitrary SQL:
    #
    #   DB['SELECT * FROM items WHERE name = ?', my_name].all
    #
    # Otherwise, acts as an alias for Database#from, setting the primary
    # table for the dataset:
    #
    #   DB[:items].sql #=> "SELECT * FROM items"
    def [](*args)
      (String === args.first) ? fetch(*args) : from(*args)
    end
    
    # Dynamically add new servers or modify server options at runtime. Also adds new
    # servers to the connection pool. Intended for use with master/slave or shard
    # configurations where it is useful to add new server hosts at runtime.
    #
    # servers argument should be a hash with server name symbol keys and hash or
    # proc values.  If a servers key is already in use, it's value is overridden
    # with the value provided.
    #
    #  DB.add_servers(:f=>{:host=>"hash_host_f"})
    def add_servers(servers)
      @opts[:servers] = @opts[:servers] ? @opts[:servers].merge(servers) : servers
      @pool.add_servers(servers.keys)
    end
    
    # Call the prepared statement with the given name with the given hash
    # of arguments.
    def call(ps_name, hash={})
      prepared_statements[ps_name].call(hash)
    end
    
    # Cast the given type to a literal type
    def cast_type_literal(type)
      type_literal(:type=>type)
    end

    # Connects to the database. This method should be overridden by descendants.
    def connect
      raise NotImplementedError, "#connect should be overridden by adapters"
    end
    
    # The database type for this database object, the same as the adapter scheme
    # by default.  Should be overridden in adapters (especially shared adapters)
    # to be the correct type, so that even if two separate Database objects are
    # using different adapters you can tell that they are using the same database
    # type.  Even better, you can tell that two Database objects that are using
    # the same adapter are connecting to different database types (think JDBC or
    # DataObjects).
    def database_type
      self.class.adapter_scheme
    end
    
    # Returns a blank dataset for this database
    def dataset
      ds = Sequel::Dataset.new(self)
    end
    
    # Disconnects all available connections from the connection pool.  Any
    # connections currently in use will not be disconnected.
    def disconnect
      pool.disconnect
    end

    # Executes the given SQL on the database. This method should be overridden in descendants.
    # This method should not be called directly by user code.
    def execute(sql, opts={})
      raise NotImplementedError, "#execute should be overridden by adapters"
    end
    
    # Method that should be used when submitting any DDL (Data Definition
    # Language) SQL.  By default, calls execute_dui.
    # This method should not be called directly by user code.
    def execute_ddl(sql, opts={}, &block)
      execute_dui(sql, opts, &block)
    end

    # Method that should be used when issuing a DELETE, UPDATE, or INSERT
    # statement.  By default, calls execute.
    # This method should not be called directly by user code.
    def execute_dui(sql, opts={}, &block)
      execute(sql, opts, &block)
    end

    # Method that should be used when issuing a INSERT
    # statement.  By default, calls execute_dui.
    # This method should not be called directly by user code.
    def execute_insert(sql, opts={}, &block)
      execute_dui(sql, opts, &block)
    end

    # Fetches records for an arbitrary SQL statement. If a block is given,
    # it is used to iterate over the records:
    #
    #   DB.fetch('SELECT * FROM items'){|r| p r}
    #
    # The method returns a dataset instance:
    #
    #   DB.fetch('SELECT * FROM items').all
    #
    # Fetch can also perform parameterized queries for protection against SQL
    # injection:
    #
    #   DB.fetch('SELECT * FROM items WHERE name = ?', my_name).all
    def fetch(sql, *args, &block)
      ds = dataset.with_sql(sql, *args)
      ds.each(&block) if block
      ds
    end
    
    # Returns a new dataset with the from method invoked. If a block is given,
    # it is used as a filter on the dataset.
    def from(*args, &block)
      ds = dataset.from(*args)
      block ? ds.filter(&block) : ds
    end
    
    # Returns a single value from the database, e.g.:
    #
    #   # SELECT 1
    #   DB.get(1) #=> 1 
    #
    #   # SELECT version()
    #   DB.get(:version.sql_function) #=> ...
    def get(*args, &block)
      dataset.get(*args, &block)
    end
    
    # The method to call on identifiers going into the database
    def identifier_input_method
      case @identifier_input_method
      when nil
        @identifier_input_method = @opts.include?(:identifier_input_method) ? @opts[:identifier_input_method] : (@@identifier_input_method.nil? ? identifier_input_method_default : @@identifier_input_method)
        @identifier_input_method == "" ? nil : @identifier_input_method
      when ""
        nil
      else
        @identifier_input_method
      end
    end
    
    # Set the method to call on identifiers going into the database
    def identifier_input_method=(v)
      reset_schema_utility_dataset
      @identifier_input_method = v || ""
    end
    
    # The method to call on identifiers coming from the database
    def identifier_output_method
      case @identifier_output_method
      when nil
        @identifier_output_method = @opts.include?(:identifier_output_method) ? @opts[:identifier_output_method] : (@@identifier_output_method.nil? ? identifier_output_method_default : @@identifier_output_method)
        @identifier_output_method == "" ? nil : @identifier_output_method
      when ""
        nil
      else
        @identifier_output_method
      end
    end
    
    # Set the method to call on identifiers coming from the database
    def identifier_output_method=(v)
      reset_schema_utility_dataset
      @identifier_output_method = v || ""
    end
    
    # Returns a string representation of the database object including the
    # class name and the connection URI (or the opts if the URI
    # cannot be constructed).
    def inspect
      "#<#{self.class}: #{(uri rescue opts).inspect}>" 
    end

    # Proxy the literal call to the dataset, used for default values.
    def literal(v)
      schema_utility_dataset.literal(v)
    end

    # Log a message at level info to all loggers.  All SQL logging
    # goes through this method.
    def log_info(message, args=nil)
      message = "#{message}; #{args.inspect}" if args
      @loggers.each{|logger| logger.info(message)}
    end

    # Remove any existing loggers and just use the given logger.
    def logger=(logger)
      @loggers = Array(logger)
    end

    # Whether to quote identifiers (columns and tables) for this database
    def quote_identifiers=(v)
      reset_schema_utility_dataset
      @quote_identifiers = v
    end
    
    # Returns true if the database quotes identifiers.
    def quote_identifiers?
      return @quote_identifiers unless @quote_identifiers.nil?
      @quote_identifiers = @opts.include?(:quote_identifiers) ? @opts[:quote_identifiers] : (@@quote_identifiers.nil? ? quote_identifiers_default : @@quote_identifiers)
    end
    
    # Runs the supplied SQL statement string on the database server. Returns nil.
    # Options:
    # * :server - The server to run the SQL on.
    def run(sql, opts={})
      execute_ddl(sql, opts)
      nil
    end
    
    # Returns a new dataset with the select method invoked.
    def select(*args, &block)
      dataset.select(*args, &block)
    end
    
    # Parse the schema from the database.
    # Returns the schema for the given table as an array with all members being arrays of length 2,
    # the first member being the column name, and the second member being a hash of column information.
    # Available options are:
    #
    # * :reload - Get fresh information from the database, instead of using
    #   cached information.  If table_name is blank, :reload should be used
    #   unless you are sure that schema has not been called before with a
    #   table_name, otherwise you may only getting the schemas for tables
    #   that have been requested explicitly.
    # * :schema - An explicit schema to use.  It may also be implicitly provided
    #   via the table name.
    def schema(table, opts={})
      raise(Error, 'schema parsing is not implemented on this database') unless respond_to?(:schema_parse_table, true)

      sch, table_name = schema_and_table(table)
      quoted_name = quote_schema_table(table)
      opts = opts.merge(:schema=>sch) if sch && !opts.include?(:schema)

      @schemas.delete(quoted_name) if opts[:reload]
      return @schemas[quoted_name] if @schemas[quoted_name]

      cols = schema_parse_table(table_name, opts)
      raise(Error, 'schema parsing returned no columns, table probably doesn\'t exist') if cols.nil? || cols.empty?
      cols.each{|_,c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      @schemas[quoted_name] = cols
    end

    # Returns true if the database is using a single-threaded connection pool.
    def single_threaded?
      @single_threaded
    end
    
    # Acquires a database connection, yielding it to the passed block.
    def synchronize(server=nil, &block)
      @pool.hold(server || :default, &block)
    end
    
    # Whether the database and adapter support savepoints, false by default
    def supports_savepoints?
      false
    end

    # Returns true if a table with the given name exists.  This requires a query
    # to the database unless this database object already has the schema for
    # the given table name.
    def table_exists?(name)
      begin 
        from(name).first
        true
      rescue
        false
      end
    end
    
    # Attempts to acquire a database connection.  Returns true if successful.
    # Will probably raise an error if unsuccessful.
    def test_connection(server=nil)
      synchronize(server){|conn|}
      true
    end

    # Starts a database transaction.  When a database transaction is used,
    # either all statements are successful or none of the statements are
    # successful.  Note that MySQL MyISAM tabels do not support transactions.
    #
    # The following options are respected:
    #
    # * :server  - The server to use for the transaction
    # * :savepoint - Whether to create a new savepoint for this transaction,
    #   only respected if the database adapter supports savepoints.  By
    #   default Sequel will reuse an existing transaction, so if you want to
    #   use a savepoint you must use this option.
    def transaction(opts={}, &block)
      synchronize(opts[:server]) do |conn|
        return yield(conn) if already_in_transaction?(conn, opts)
        _transaction(conn, &block)
      end
    end
    
    # Typecast the value to the given column_type. Calls
    # typecast_value_#{column_type} if the method exists,
    # otherwise returns the value.
    # This method should raise Sequel::InvalidValue if assigned value
    # is invalid.
    def typecast_value(column_type, value)
      return nil if value.nil?
      meth = "typecast_value_#{column_type}"
      begin
        respond_to?(meth, true) ? send(meth, value) : value
      rescue ArgumentError, TypeError => e
        raise Sequel.convert_exception_class(e, InvalidValue)
      end
    end
    
    # Returns the URI identifying the database.
    # This method can raise an error if the database used options
    # instead of a connection string.
    def uri
      uri = URI::Generic.new(
        self.class.adapter_scheme.to_s,
        nil,
        @opts[:host],
        @opts[:port],
        nil,
        "/#{@opts[:database]}",
        nil,
        nil,
        nil
      )
      uri.user = @opts[:user]
      uri.password = @opts[:password] if uri.user
      uri.to_s
    end
    
    # Explicit alias of uri for easier subclassing.
    def url
      uri
    end
    
    private
    
    # Internal generic transaction method.  Any exception raised by the given
    # block will cause the transaction to be rolled back.  If the exception is
    # not Sequel::Rollback, the error will be reraised. If no exception occurs
    # inside the block, the transaction is commited.
    def _transaction(conn)
      begin
        add_transaction
        t = begin_transaction(conn)
        yield(conn)
      rescue Exception => e
        rollback_transaction(t) if t
        transaction_error(e)
      ensure
        begin
          commit_transaction(t) unless e
        rescue Exception => e
          raise_error(e, :classes=>database_error_classes)
        ensure
          remove_transaction(t)
        end
      end
    end
    
    # Add the current thread to the list of active transactions
    def add_transaction
      th = Thread.current
      if supports_savepoints?
        unless @transactions.include?(th)
          th[:sequel_transaction_depth] = 0
          @transactions << th
        end
      else
        @transactions << th
      end
    end    

    # Whether the current thread/connection is already inside a transaction
    def already_in_transaction?(conn, opts)
      @transactions.include?(Thread.current) && (!supports_savepoints? || !opts[:savepoint])
    end
    
    # SQL to start a new savepoint
    def begin_savepoint_sql(depth)
      SQL_SAVEPOINT % depth
    end

    # Start a new database transaction on the given connection.
    def begin_transaction(conn)
      if supports_savepoints?
        th = Thread.current
        depth = th[:sequel_transaction_depth]
        conn = transaction_statement_object(conn) if respond_to?(:transaction_statement_object, true)
        log_connection_execute(conn, depth > 0 ? begin_savepoint_sql(depth) : begin_transaction_sql)
        th[:sequel_transaction_depth] += 1
      else
        log_connection_execute(conn, begin_transaction_sql)
      end
      conn
    end
    
    # SQL to BEGIN a transaction.
    def begin_transaction_sql
      SQL_BEGIN
    end

    # Returns true when the object is considered blank.
    # The only objects that are blank are nil, false,
    # strings with all whitespace, and ones that respond
    # true to empty?
    def blank_object?(obj)
      return obj.blank? if obj.respond_to?(:blank?)
      case obj
      when NilClass, FalseClass
        true
      when Numeric, TrueClass
        false
      when String
        obj.strip.empty?
      else
        obj.respond_to?(:empty?) ? obj.empty? : false
      end
    end
    
    # Convert the given default, which should be a database specific string, into
    # a ruby object.
    def column_schema_to_ruby_default(default, type)
      return if default.nil?
      orig_default = default
      if database_type == :postgres and m = POSTGRES_DEFAULT_RE.match(default)
        default = m[1] || m[2]
      end
      if database_type == :mssql and m = MSSQL_DEFAULT_RE.match(default)
        default = m[1] || m[2]
      end
      if [:string, :blob, :date, :datetime, :time, :enum].include?(type)
        if database_type == :mysql
          return if [:date, :datetime, :time].include?(type) && MYSQL_TIMESTAMP_RE.match(default)
          orig_default = default = "'#{default.gsub("'", "''").gsub('\\', '\\\\')}'"
        end
        return unless m = STRING_DEFAULT_RE.match(default)
        default = m[1].gsub("''", "'")
      end
      res = begin
        case type
        when :boolean
          case default 
          when /[f0]/i
            false
          when /[t1]/i
            true
          end
        when :string, :enum
          default
        when :blob
          Sequel::SQL::Blob.new(default)
        when :integer
          Integer(default)
        when :float
          Float(default)
        when :date
          Sequel.string_to_date(default)
        when :datetime
          DateTime.parse(default)
        when :time
          Sequel.string_to_time(default)
        when :decimal
          BigDecimal.new(default)
        end
      rescue
        nil
      end
    end
   
    # SQL to commit a savepoint
    def commit_savepoint_sql(depth)
      SQL_RELEASE_SAVEPOINT % depth
    end

    # Commit the active transaction on the connection
    def commit_transaction(conn)
      if supports_savepoints?
        depth = Thread.current[:sequel_transaction_depth]
        log_connection_execute(conn, depth > 1 ? commit_savepoint_sql(depth-1) : commit_transaction_sql)
      else
        log_connection_execute(conn, commit_transaction_sql)
      end
    end

    # SQL to COMMIT a transaction.
    def commit_transaction_sql
      SQL_COMMIT
    end
    
    # Method called on the connection object to execute SQL on the database,
    # used by the transaction code.
    def connection_execute_method
      :execute
    end

    # The default options for the connection pool.
    def connection_pool_default_options
      {}
    end
    
    # Which transaction errors to translate, blank by default.
    def database_error_classes
      []
    end
    
    # The default value for default_schema.
    def default_schema_default
      nil
    end

    # The method to apply to identifiers going into the database by default.
    # Should be overridden in subclasses for databases that fold unquoted
    # identifiers to lower case instead of uppercase, such as
    # MySQL, PostgreSQL, and SQLite.
    def identifier_input_method_default
      :upcase
    end
    
    # The method to apply to identifiers coming the database by default.
    # Should be overridden in subclasses for databases that fold unquoted
    # identifiers to lower case instead of uppercase, such as
    # MySQL, PostgreSQL, and SQLite.
    def identifier_output_method_default
      :downcase
    end
    
    # Return a Method object for the dataset's output_identifier_method.
    # Used in metadata parsing to make sure the returned information is in the
    # correct format.
    def input_identifier_meth
      dataset.method(:input_identifier)
    end
    
    # Log the given SQL and then execute it on the connection, used by
    # the transaction code.
    def log_connection_execute(conn, sql)
      log_info(sql)
      conn.send(connection_execute_method, sql)
    end

    # Return a dataset that uses the default identifier input and output methods
    # for this database.  Used when parsing metadata so that column symbols are
    # returned as expected.
    def metadata_dataset
      return @metadata_dataset if @metadata_dataset
      ds = dataset
      ds.identifier_input_method = identifier_input_method_default
      ds.identifier_output_method = identifier_output_method_default
      @metadata_dataset = ds
    end

    # Return a Method object for the dataset's output_identifier_method.
    # Used in metadata parsing to make sure the returned information is in the
    # correct format.
    def output_identifier_meth
      dataset.method(:output_identifier)
    end

    # Whether to quote identifiers by default for this database, true
    # by default.
    def quote_identifiers_default
      true
    end

    # SQL to ROLLBACK a transaction.
    def rollback_transaction_sql
      SQL_ROLLBACK
    end
    
    # Convert the given exception to a DatabaseError, keeping message
    # and traceback.
    def raise_error(exception, opts={})
      if !opts[:classes] || Array(opts[:classes]).any?{|c| exception.is_a?(c)}
        raise Sequel.convert_exception_class(exception, opts[:disconnect] ? DatabaseDisconnectError : DatabaseError)
      else
        raise exception
      end
    end
    
    # Remove the cached schema for the given schema name
    def remove_cached_schema(table)
      @schemas.delete(quote_schema_table(table)) if @schemas
    end
    
    # Remove the current thread from the list of active transactions
    def remove_transaction(conn)
      th = Thread.current
      @transactions.delete(th) if !supports_savepoints? || ((th[:sequel_transaction_depth] -= 1) <= 0)
    end

    # Remove the cached schema_utility_dataset, because the identifier
    # quoting has changed.
    def reset_schema_utility_dataset
      @schema_utility_dataset = nil
    end
    
    # SQL to rollback to a savepoint
    def rollback_savepoint_sql(depth)
      SQL_ROLLBACK_TO_SAVEPOINT % depth
    end

    # Rollback the active transaction on the connection
    def rollback_transaction(conn)
      if supports_savepoints?
        depth = Thread.current[:sequel_transaction_depth]
        log_connection_execute(conn, depth > 1 ? rollback_savepoint_sql(depth-1) : rollback_transaction_sql)
      else
        log_connection_execute(conn, rollback_transaction_sql)
      end
    end

    # Split the schema information from the table
    def schema_and_table(table_name)
      schema_utility_dataset.schema_and_table(table_name)
    end

    # Return true if the given column schema represents an autoincrementing primary key.
    def schema_autoincrementing_primary_key?(schema)
      !!schema[:primary_key]
    end

    # Match the database's column type to a ruby type via a
    # regular expression.  The following ruby types are supported:
    # integer, string, date, datetime, boolean, and float.
    def schema_column_type(db_type)
      case db_type
      when /\Ainterval\z/io
        :interval
      when /\A(character( varying)?|n?(var)?char|n?text)/io
        :string
      when /\A(int(eger)?|(big|small|tiny)int)/io
        :integer
      when /\Adate\z/io
        :date
      when /\A((small)?datetime|timestamp( with(out)? time zone)?)\z/io
        :datetime
      when /\Atime( with(out)? time zone)?\z/io
        :time
      when /\A(boolean|bit)\z/io
        :boolean
      when /\A(real|float|double( precision)?)\z/io
        :float
      when /\A(((numeric|decimal)(\(\d+,\d+\))?)|(small)?money)\z/io
        :decimal
      when /bytea|blob|image|(var)?binary/io
        :blob
      when /\Aenum/
        :enum
      end
    end

    # The dataset to use for proxying certain schema methods.
    def schema_utility_dataset
      @schema_utility_dataset ||= dataset
    end

    # Return the options for the given server by merging the generic
    # options for all server with the specific options for the given
    # server specified in the :servers option.
    def server_opts(server)
      opts = if @opts[:servers] && server_options = @opts[:servers][server]
        case server_options
        when Hash
          @opts.merge(server_options)
        when Proc
          @opts.merge(server_options.call(self))
        else
          raise Error, 'Server opts should be a hash or proc'
        end
      else
        @opts.dup
      end
      opts.delete(:servers)
      opts
    end
    
    # Raise a database error unless the exception is an Rollback.
    def transaction_error(e)
      raise_error(e, :classes=>database_error_classes) unless e.is_a?(Rollback)
    end

    # Typecast the value to an SQL::Blob
    def typecast_value_blob(value)
      value.is_a?(Sequel::SQL::Blob) ? value : Sequel::SQL::Blob.new(value)
    end

    # Typecast the value to true, false, or nil
    def typecast_value_boolean(value)
      case value
      when false, 0, "0", /\Af(alse)?\z/i
        false
      else
        blank_object?(value) ? nil : true
      end
    end

    # Typecast the value to a Date
    def typecast_value_date(value)
      case value
      when Date
        value
      when DateTime, Time
        Date.new(value.year, value.month, value.day)
      when String
        Sequel.string_to_date(value)
      when Hash
        Date.new(*[:year, :month, :day].map{|x| (value[x] || value[x.to_s]).to_i})
      else
        raise InvalidValue, "invalid value for Date: #{value.inspect}"
      end
    end

    # Typecast the value to a DateTime or Time depending on Sequel.datetime_class
    def typecast_value_datetime(value)
      raise(Sequel::InvalidValue, "invalid value for Datetime: #{value.inspect}") unless [DateTime, Date, Time, String, Hash].any?{|c| value.is_a?(c)}
      klass = Sequel.datetime_class
      if value.is_a?(Hash)
        klass.send(klass == Time ? :mktime : :new, *[:year, :month, :day, :hour, :minute, :second].map{|x| (value[x] || value[x.to_s]).to_i})
      else
        Sequel.typecast_to_application_timestamp(value)
      end
    end

    # Typecast the value to a BigDecimal
    def typecast_value_decimal(value)
      case value
      when BigDecimal
        value
      when String, Numeric
        BigDecimal.new(value.to_s)
      else
        raise InvalidValue, "invalid value for BigDecimal: #{value.inspect}"
      end
    end

    # Typecast the value to a Float
    def typecast_value_float(value)
      Float(value)
    end

    # Typecast the value to an Integer
    def typecast_value_integer(value)
      Integer(value)
    end

    # Typecast the value to a String
    def typecast_value_string(value)
      value.to_s
    end

    # Typecast the value to a Time
    def typecast_value_time(value)
      case value
      when Time
        value
      when String
        Sequel.string_to_time(value)
      when Hash
        t = Time.now
        Time.mktime(t.year, t.month, t.day, *[:hour, :minute, :second].map{|x| (value[x] || value[x.to_s]).to_i})
      else
        raise Sequel::InvalidValue, "invalid value for Time: #{value.inspect}"
      end
    end
  end
end

