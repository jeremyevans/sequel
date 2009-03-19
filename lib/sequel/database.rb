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
    ADAPTERS = %w'ado db2 dbi do firebird informix jdbc mysql odbc openbase oracle postgres sqlite'.collect{|x| x.to_sym}

    SQL_BEGIN = 'BEGIN'.freeze
    SQL_COMMIT = 'COMMIT'.freeze
    SQL_ROLLBACK = 'ROLLBACK'.freeze

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
      
      @single_threaded = opts.include?(:single_threaded) ? opts[:single_threaded] : @@single_threaded
      @schemas = nil
      @default_schema = opts.include?(:default_schema) ? opts[:default_schema] : default_schema_default
      @prepared_statements = {}
      @transactions = []
      @identifier_input_method = nil
      @identifier_output_method = nil
      @quote_identifiers = nil
      if opts.include?(:upcase_identifiers)
        Deprecation.deprecate('The :upcase_identifiers Database option', 'Use the :identifier_input_method => :upcase option instead')
        @identifier_input_method = opts[:upcase_identifiers] ? :upcase : ""
      end
      @pool = (@single_threaded ? SingleThreadedPool : ConnectionPool).new(connection_pool_default_options.merge(opts), &block)
      @pool.connection_proc = proc{|server| connect(server)} unless block
      @pool.disconnection_proc = proc{|conn| disconnect_connection(conn)} unless opts[:disconnection_proc]

      @loggers = Array(opts[:logger]) + Array(opts[:loggers])
      ::Sequel::DATABASES.push(self)
    end
    
    ### Class Methods ###

    # The Database subclass for the given adapter scheme.
    # Raises Sequel::Error::AdapterNotFound if the adapter
    # could not be loaded.
    def self.adapter_class(scheme)
      scheme = scheme.to_s.gsub('-', '_').to_sym
      
      unless klass = ADAPTER_MAP[scheme]
        # attempt to load the adapter file
        begin
          Sequel.require "adapters/#{scheme}"
        rescue LoadError => e
          raise Error::AdapterNotFound, "Could not load #{scheme} adapter:\n  #{e.message}"
        end
        
        # make sure we actually loaded the adapter
        unless klass = ADAPTER_MAP[scheme]
          raise Error::AdapterNotFound, "Could not load #{scheme} adapter"
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
      if conn_string.is_a?(String)
        if match = /\A(jdbc|do):/o.match(conn_string)
          c = adapter_class(match[1].to_sym)
          opts = {:uri=>conn_string}.merge(opts)
        else
          uri = URI.parse(conn_string)
          scheme = uri.scheme
          scheme = :dbi if scheme =~ /\Adbi-/
          c = adapter_class(scheme)
          uri_options = {}
          uri.query.split('&').collect{|s| s.split('=')}.each{|k,v| uri_options[k.to_sym] = v} unless uri.query.to_s.strip.empty?
          opts = c.send(:uri_to_options, uri).merge(uri_options).merge(opts)
        end
      else
        opts = conn_string.merge(opts)
        c = adapter_class(opts[:adapter] || opts['adapter'])
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

    # Executes the supplied SQL statement string.
    def <<(sql)
      Deprecation.deprecate('Passing an array argument to Database#<<', 'Use array.each{|x| database << x}') if Array === sql
      execute_ddl((Array === sql) ? sql.to_sql : sql)
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
    
    # Call the prepared statement with the given name with the given hash
    # of arguments.
    def call(ps_name, hash={})
      prepared_statements[ps_name].call(hash)
    end
    
    # Connects to the database. This method should be overridden by descendants.
    def connect
      raise NotImplementedError, "#connect should be overridden by adapters"
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
    
    # Returns a new dataset with the select method invoked.
    def select(*args, &block)
      dataset.select(*args, &block)
    end
    
    # Parse the schema from the database.
    # If the table_name is not given, returns the schema for all tables as a hash.
    # If the table_name is given, returns the schema for a single table as an
    # array with all members being arrays of length 2.  Available options are:
    #
    # * :reload - Get fresh information from the database, instead of using
    #   cached information.  If table_name is blank, :reload should be used
    #   unless you are sure that schema has not been called before with a
    #   table_name, otherwise you may only getting the schemas for tables
    #   that have been requested explicitly.
    # * :schema - An explicit schema to use.  It may also be implicitly provided
    #   via the table name.
    def schema(table = nil, opts={})
      Deprecation.deprecate('Calling Database#schema without a table argument', 'Use database.tables.inject({}){|h, m| h[m] = database.schema(m); h}') unless table
      raise(Error, 'schema parsing is not implemented on this database') unless respond_to?(:schema_parse_table, true)

      if table
        sch, table_name = schema_and_table(table)
        quoted_name = quote_schema_table(table)
      end
      opts = opts.merge(:schema=>sch) if sch && !opts.include?(:schema)
      if opts[:reload] && @schemas
        if table_name
          @schemas.delete(quoted_name)
        else
          @schemas = nil
        end
      end

      if @schemas
        if table_name
          return @schemas[quoted_name] if @schemas[quoted_name]
        else
          return @schemas
        end
      end

      raise(Error, '#tables does not exist, you must provide a specific table to #schema') if table.nil? && !respond_to?(:tables, true)

      @schemas ||= Hash.new do |h,k|
        quote_name = quote_schema_table(k)
        h[quote_name] if h.include?(quote_name)
      end

      if table_name
        cols = schema_parse_table(table_name, opts)
        raise(Error, 'schema parsing returned no columns, table probably doesn\'t exist') if cols.nil? || cols.empty?
        @schemas[quoted_name] = cols
      else
        tables.each{|t| @schemas[quote_schema_table(t)] = schema_parse_table(t.to_s, opts)}
        @schemas
      end
    end

    # Returns true if the database is using a single-threaded connection pool.
    def single_threaded?
      @single_threaded
    end
    
    # Acquires a database connection, yielding it to the passed block.
    def synchronize(server=nil, &block)
      @pool.hold(server || :default, &block)
    end

    # Returns true if a table with the given name exists.  This requires a query
    # to the database unless this database object already has the schema for
    # the given table name.
    def table_exists?(name)
      if @schemas && @schemas[name]
        true
      else
        begin 
          from(name).first
          true
        rescue
          false
        end
      end
    end
    
    # Attempts to acquire a database connection.  Returns true if successful.
    # Will probably raise an error if unsuccessful.
    def test_connection(server=nil)
      synchronize(server){|conn|}
      true
    end

    # A simple implementation of SQL transactions. Nested transactions are not 
    # supported - calling #transaction within a transaction will reuse the 
    # current transaction. Should be overridden for databases that support nested 
    # transactions.
    def transaction(opts={})
      unless opts.is_a?(Hash)
        Deprecation.deprecate('Passing an argument other than a Hash to Database#transaction', "Use DB.transaction(:server=>#{opts.inspect})") 
        opts = {:server=>opts}
      end
      synchronize(opts[:server]) do |conn|
        return yield(conn) if @transactions.include?(Thread.current)
        log_info(begin_transaction_sql)
        conn.execute(begin_transaction_sql)
        begin
          @transactions << Thread.current
          yield(conn)
        rescue Exception => e
          log_info(rollback_transaction_sql)
          conn.execute(rollback_transaction_sql)
          transaction_error(e)
        ensure
          unless e
            log_info(commit_transaction_sql)
            conn.execute(commit_transaction_sql)
          end
          @transactions.delete(Thread.current)
        end
      end
    end
    
    # Typecast the value to the given column_type. Can be overridden in
    # adapters to support database specific column types.
    # This method should raise Sequel::Error::InvalidValue if assigned value
    # is invalid.
    def typecast_value(column_type, value)
      return nil if value.nil?
      begin
        case column_type
        when :integer
          Integer(value)
        when :string
          value.to_s
        when :float
          Float(value)
        when :decimal
          case value
          when BigDecimal
            value
          when String, Numeric
            BigDecimal.new(value.to_s)
          else
            raise Sequel::Error::InvalidValue, "invalid value for BigDecimal: #{value.inspect}"
          end
        when :boolean
          case value
          when false, 0, "0", /\Af(alse)?\z/i
            false
          else
            blank_object?(value) ? nil : true
          end
        when :date
          case value
          when Date
            value
          when DateTime, Time
            Date.new(value.year, value.month, value.day)
          when String
            Sequel.string_to_date(value)
          else
            raise Sequel::Error::InvalidValue, "invalid value for Date: #{value.inspect}"
          end
        when :time
          case value
          when Time
            value
          when String
            Sequel.string_to_time(value)
          else
            raise Sequel::Error::InvalidValue, "invalid value for Time: #{value.inspect}"
          end
        when :datetime
          raise(Sequel::Error::InvalidValue, "invalid value for Datetime: #{value.inspect}") unless [DateTime, Date, Time, String].any?{|c| value.is_a?(c)}
          if Sequel.datetime_class === value
            # Already the correct class, no need to convert
            value
          else
            # First convert it to standard ISO 8601 time, then
            # parse that string using the time class.
            Sequel.string_to_datetime(Time === value ? value.iso8601 : value.to_s)
          end
        when :blob
          value.is_a?(Sequel::SQL::Blob) ? value : Sequel::SQL::Blob.new(value)
        else
          value
        end
      rescue ArgumentError, TypeError => exp
        e = Sequel::Error::InvalidValue.new("#{exp.class} #{exp.message}")
        e.set_backtrace(exp.backtrace)
        raise e
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
    
    # SQL to BEGIN a transaction.
    def begin_transaction_sql
      SQL_BEGIN
    end

    # Returns true when the object is considered blank.
    # The only objects that are blank are nil, false,
    # strings with all whitespace, and ones that respond
    # true to empty?
    def blank_object?(obj)
      case obj
      when NilClass, FalseClass
        true
      when Numeric, TrueClass
        false
      when String
        obj.strip.empty?
      else
        !obj.respond_to?(:empty?) || obj.empty?
      end
    end

    # SQL to COMMIT a transaction.
    def commit_transaction_sql
      SQL_COMMIT
    end

    # The default options for the connection pool.
    def connection_pool_default_options
      {}
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
        e = DatabaseError.new("#{exception.class} #{exception.message}")
        e.set_backtrace(exception.backtrace)
        raise e
      else
        raise exception
      end
    end
    
    # Remove the cached schema for the given schema name
    def remove_cached_schema(table)
      @schemas.delete(quote_schema_table(table)) if @schemas
    end

    # Remove the cached schema_utility_dataset, because the identifier
    # quoting has changed.
    def reset_schema_utility_dataset
      @schema_utility_dataset = nil
    end

    # Split the schema information from the table
    def schema_and_table(table_name)
      schema_utility_dataset.schema_and_table(table_name)
    end

    # Match the database's column type to a ruby type via a
    # regular expression.  The following ruby types are supported:
    # integer, string, date, datetime, boolean, and float.
    def schema_column_type(db_type)
      case db_type
      when /\Atinyint/io
        Sequel.convert_tinyint_to_bool ? :boolean : :integer
      when /\Ainterval\z/io
        :interval
      when /\A(character( varying)?|varchar|text)/io
        :string
      when /\A(int(eger)?|bigint|smallint)/io
        :integer
      when /\Adate\z/io
        :date
      when /\A(datetime|timestamp( with(out)? time zone)?)\z/io
        :datetime
      when /\Atime( with(out)? time zone)?\z/io
        :time
      when /\Aboolean\z/io
        :boolean
      when /\A(real|float|double( precision)?)\z/io
        :float
      when /\A(numeric(\(\d+,\d+\))?|decimal|money)\z/io
        :decimal
      when /bytea|blob/io
        :blob
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

    # Raise a database error unless the exception is an Error::Rollback.
    def transaction_error(e, *classes)
      raise_error(e, :classes=>classes) unless Error::Rollback === e
    end
  end
end

