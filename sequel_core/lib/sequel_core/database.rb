require 'sequel_core/database/schema'

module Sequel
  DATABASES = []

  # A Database object represents a virtual connection to a database.
  # The Database class is meant to be subclassed by database adapters in order
  # to provide the functionality needed for executing queries.
  class Database
    include Schema::SQL

    ADAPTERS = %w'ado db2 dbi informix jdbc mysql odbc odbc_mssql openbase oracle postgres sqlite'.collect{|x| x.to_sym}
    SQL_BEGIN = 'BEGIN'.freeze
    SQL_COMMIT = 'COMMIT'.freeze
    SQL_ROLLBACK = 'ROLLBACK'.freeze

    @@adapters = Hash.new
    @@single_threaded = false
    @@quote_identifiers = true

    attr_accessor :loggers
    attr_reader :opts, :pool
    attr_writer :quote_identifiers

    # Constructs a new instance of a database connection with the specified
    # options hash.
    #
    # Sequel::Database is an abstract class that is not useful by itself.
    def initialize(opts = {}, &block)
      @opts = opts
      
      @quote_identifiers = opts[:quote_identifiers] || @@quote_identifiers
      @single_threaded = opts[:single_threaded] || @@single_threaded
      @schemas = nil
      @pool = (@single_threaded ? SingleThreadedPool : ConnectionPool).new(connection_pool_default_options.merge(opts), &block)
      @pool.connection_proc = proc {connect} unless block

      @loggers = Array(opts[:logger]) + Array(opts[:loggers])
      ::Sequel::DATABASES.push(self)
    end
    
    ### Class Methods ###

    def self.adapter_class(scheme)
      scheme = scheme.to_sym
      
      if (klass = @@adapters[scheme]).nil?
        # attempt to load the adapter file
        begin
          require File.join(File.dirname(__FILE__), "adapters/#{scheme}")
        rescue LoadError => e
          raise Error::AdapterNotFound, "Could not load #{scheme} adapter:\n  #{e.message}"
        end
        
        # make sure we actually loaded the adapter
        if (klass = @@adapters[scheme]).nil?
          raise Error::AdapterNotFound, "Could not load #{scheme} adapter"
        end
      end
      return klass
    end
        
    # Returns the scheme for the Database class.
    def self.adapter_scheme
      @scheme
    end
    
    # call-seq:
    #   Sequel::Database.connect(conn_string)
    #   Sequel::Database.connect(opts)
    #   Sequel.connect(conn_string)
    #   Sequel.connect(opts)
    #   Sequel.open(conn_string)
    #   Sequel.open(opts)
    #
    # Creates a new database object based on the supplied connection string
    # and or options. If a URI is used, the URI scheme determines the database
    # class used, and the rest of the string specifies the connection options. 
    # For example:
    #
    #   DB = Sequel.open 'sqlite://blog.db'
    #   # opens database at ./blog.db
    #
    # The second form of this method takes an options:
    #
    #   DB = Sequel.open :adapter => :sqlite, :database => 'blog.db'
    def self.connect(conn_string, opts = nil, &block)
      if conn_string.is_a?(String)
        uri = URI.parse(conn_string)
        scheme = uri.scheme
        scheme = :dbi if scheme =~ /^dbi-(.+)/
        c = adapter_class(scheme)
        opts = c.uri_to_options(uri).merge(opts || {})
      else
        opts = conn_string.merge(opts || {})
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
        end
        nil
      else
        c.new(opts)
      end
    end

    # Sets the default quote_identifiers mode for new databases.
    def self.quote_identifiers=(value)
      @@quote_identifiers = value
    end

    # Sets the default single_threaded mode for new databases.
    def self.single_threaded=(value)
      @@single_threaded = value
    end

    # Converts a uri to an options hash. These options are then passed
    # to a newly created database object.
    def self.uri_to_options(uri)
      if uri.is_a?(String)
        uri = URI.parse(uri)
      end
      # special case for sqlite
      if uri.scheme == 'sqlite'
        {
          :user => uri.user,
          :password => uri.password,
          :database => (uri.host.nil? && uri.path == '/') ? nil : "#{uri.host}#{uri.path}"
        }
      else
        {
          :user => uri.user,
          :password => uri.password,
          :host => uri.host,
          :port => uri.port,
          :database => (m = /\/(.*)/.match(uri.path)) && (m[1])
        }
      end
    end
    
    ### Private Class Methods ###

    # Sets the adapter scheme for the Database class. Call this method in
    # descendnants of Database to allow connection using a URL. For example the
    # following:
    #   class DB2::Database < Sequel::Database
    #     set_adapter_scheme :db2
    #     ...
    #   end
    # would allow connection using:
    #   Sequel.open('db2://user:password@dbserver/mydb')
    def self.set_adapter_scheme(scheme)
      @scheme = scheme
      @@adapters[scheme.to_sym] = self
    end
    metaprivate :set_adapter_scheme
    
    ### Instance Methods ###

    # Executes the supplied SQL statement. The SQL can be supplied as a string
    # or as an array of strings. If an array is give, comments and excessive 
    # white space are removed. See also Array#to_sql.
    def <<(sql)
      execute((Array === sql) ? sql.to_sql : sql)
    end
    
    # Returns a dataset from the database. If the first argument is a string,
    # the method acts as an alias for Database#fetch, returning a dataset for
    # arbitrary SQL:
    #
    #   DB['SELECT * FROM items WHERE name = ?', my_name].print
    #
    # Otherwise, the dataset returned has its from option set to the given
    # arguments:
    #
    #   DB[:items].sql #=> "SELECT * FROM items"
    #
    def [](*args)
      (String === args.first) ? fetch(*args) : from(*args)
    end
    
    # Connects to the database. This method should be overriden by descendants.
    def connect
      raise NotImplementedError, "#connect should be overriden by adapters"
    end
    
    # Returns a blank dataset
    def dataset
      ds = Sequel::Dataset.new(self)
    end
    
    # Disconnects from the database. This method should be overriden by 
    # descendants.
    def disconnect
      raise NotImplementedError, "#disconnect should be overriden by adapters"
    end

    # Raises a Sequel::Error::NotImplemented. This method is overriden in descendants.
    def execute(sql)
      raise NotImplementedError, "#execute should be overriden by adapters"
    end
    
    # Fetches records for an arbitrary SQL statement. If a block is given,
    # it is used to iterate over the records:
    #
    #   DB.fetch('SELECT * FROM items') {|r| p r}
    #
    # If a block is not given, the method returns a dataset instance:
    #
    #   DB.fetch('SELECT * FROM items').print
    #
    # Fetch can also perform parameterized queries for protection against SQL
    # injection:
    #
    #   DB.fetch('SELECT * FROM items WHERE name = ?', my_name).print
    #
    # A short-hand form for Database#fetch is Database#[]:
    #
    #   DB['SELECT * FROM items'].each {|r| p r}
    #
    def fetch(sql, *args, &block)
      ds = dataset
      sql = sql.gsub('?') {|m|  ds.literal(args.shift)}
      if block
        ds.fetch_rows(sql, &block)
      else
        ds.opts[:sql] = sql
        ds
      end
    end
    alias_method :>>, :fetch
    
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
    #   DB.get(:version[]) #=> ...
    def get(expr)
      dataset.get(expr)
    end
    
    # Returns a string representation of the database object including the
    # class name and the connection URI.
    def inspect
      "#<#{self.class}: #{(uri rescue opts).inspect}>" 
    end

    # Log a message at level info to all loggers
    def log_info(message)
      @loggers.each{|logger| logger.info(message)}
    end

    # Return the first logger, if any.  Should only be used for backwards
    # compatibility.
    def logger
      @loggers.first
    end

    # Replace the array of loggers with the given logger(s)
    def logger=(logger)
      @loggers = Array(logger)
    end

    # Returns true if the database is using a multi-threaded connection pool.
    def multi_threaded?
      !@single_threaded
    end
    
    # Converts a query block into a dataset. For more information see 
    # Dataset#query.
    def query(&block)
      dataset.query(&block)
    end
    
    # Returns true if the database quotes identifiers
    def quote_identifiers?
      @quote_identifiers
    end
    
    # Returns a new dataset with the select method invoked.
    def select(*args)
      dataset.select(*args)
    end
    
    # default serial primary key definition. this should be overriden for each adapter.
    def serial_primary_key_options
      {:primary_key => true, :type => :integer, :auto_increment => true}
    end
    
    # Returns true if the database is using a single-threaded connection pool.
    def single_threaded?
      @single_threaded
    end
    
    # Acquires a database connection, yielding it to the passed block.
    def synchronize(&block)
      @pool.hold(&block)
    end

    # Returns true if the given table exists.
    def table_exists?(name)
      begin 
        if respond_to?(:tables)
          tables.include?(name.to_sym)
        else
          from(name).first
          true
        end
      rescue
        false
      end
    end
    
    # Returns true if there is a database connection
    def test_connection
      @pool.hold {|conn|}
      true
    end
    
    # A simple implementation of SQL transactions. Nested transactions are not 
    # supported - calling #transaction within a transaction will reuse the 
    # current transaction. May be overridden for databases that support nested 
    # transactions.
    def transaction
      @pool.hold do |conn|
        @transactions ||= []
        if @transactions.include? Thread.current
          return yield(conn)
        end
        log_info(SQL_BEGIN)
        conn.execute(SQL_BEGIN)
        begin
          @transactions << Thread.current
          yield(conn)
        rescue Exception => e
          log_info(SQL_ROLLBACK)
          conn.execute(SQL_ROLLBACK)
          raise e unless Error::Rollback === e
        ensure
          unless e
            log_info(SQL_COMMIT)
            conn.execute(SQL_COMMIT)
          end
          @transactions.delete(Thread.current)
        end
      end
    end
    
    # Typecast the value to the given column_type
    def typecast_value(column_type, value)
      return nil if value.nil?
      case column_type
      when :integer
        Integer(value)
      when :string
        value.to_s
      when :float
        Float(value)
      when :boolean
        case value
        when false, 0, "0", /\Af(alse)?\z/i
          false
        else
          value.blank? ? nil : true
        end
      when :date
        case value
        when Date
          value
        when DateTime, Time
          Date.new(value.year, value.month, value.day)
        when String
          value.to_date
        else
          raise ArgumentError, "invalid value for Date: #{value.inspect}"
        end
      when :datetime
        case value
        when DateTime
          value
        when Date
          DateTime.new(value.year, value.month, value.day)
        when Time
          DateTime.new(value.year, value.month, value.day, value.hour, value.min, value.sec)
        when String
          value.to_datetime
        else
          raise ArgumentError, "invalid value for DateTime: #{value.inspect}"
        end
      else
        value
      end
    end 

    # Returns the URI identifying the database.
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
    alias url uri # Because I don't care much for the semantic difference.
    
    private

    def connection_pool_default_options
      {}
    end
  end
end

