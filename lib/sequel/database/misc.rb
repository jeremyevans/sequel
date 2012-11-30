module Sequel
  class Database
    # ---------------------
    # :section: 7 - Miscellaneous methods
    # These methods don't fit neatly into another category.
    # ---------------------
    
    # Hash of extension name symbols to callable objects to load the extension
    # into the Database object (usually by extending it with a module defined
    # in the extension).
    EXTENSIONS = {}

    # The general default size for string columns for all Sequel::Database
    # instances.
    DEFAULT_STRING_COLUMN_SIZE = 255

    # Register an extension callback for Database objects.  ext should be the
    # extension name symbol, and mod should either be a Module that the
    # database is extended with, or a callable object called with the database
    # object.  If mod is not provided, a block can be provided and is treated
    # as the mod object.
    def self.register_extension(ext, mod=nil, &block)
      if mod
        raise(Error, "cannot provide both mod and block to Database.register_extension") if block
        if mod.is_a?(Module)
          block = proc{|db| db.extend(mod)}
        else
          block = mod
        end
      end
      Sequel.synchronize{EXTENSIONS[ext] = block}
    end

    # Converts a uri to an options hash. These options are then passed
    # to a newly created database object. 
    def self.uri_to_options(uri)
      { :user => uri.user,
        :password => uri.password,
        :host => uri.host,
        :port => uri.port,
        :database => (m = /\/(.*)/.match(uri.path)) && (m[1]) }
    end
    private_class_method :uri_to_options
    
    # The options hash for this database
    attr_reader :opts
    
    # Set the timezone to use for this database, overridding <tt>Sequel.database_timezone</tt>.
    attr_writer :timezone
    
    # The specific default size of string columns for this Sequel::Database, usually 255 by default.
    attr_accessor :default_string_column_size

    # Constructs a new instance of a database connection with the specified
    # options hash.
    #
    # Accepts the following options:
    # :default_schema :: The default schema to use, see #default_schema.
    # :default_string_column_size :: The default size of string columns, 255 by default.
    # :identifier_input_method :: A string method symbol to call on identifiers going into the database
    # :identifier_output_method :: A string method symbol to call on identifiers coming from the database
    # :logger :: A specific logger to use
    # :loggers :: An array of loggers to use
    # :quote_identifiers :: Whether to quote identifiers
    # :servers :: A hash specifying a server/shard specific options, keyed by shard symbol 
    # :single_threaded :: Whether to use a single-threaded connection pool
    # :sql_log_level :: Method to use to log SQL to a logger, :info by default.
    #
    # All options given are also passed to the connection pool.
    def initialize(opts = {}, &block)
      @opts ||= opts
      @opts = connection_pool_default_options.merge(@opts)
      @loggers = Array(@opts[:logger]) + Array(@opts[:loggers])
      self.log_warn_duration = @opts[:log_warn_duration]
      block ||= proc{|server| connect(server)}
      @opts[:servers] = {} if @opts[:servers].is_a?(String)
      @opts[:adapter_class] = self.class
      
      @opts[:single_threaded] = @single_threaded = typecast_value_boolean(@opts.fetch(:single_threaded, @@single_threaded))
      @schemas = {}
      @default_schema = @opts.fetch(:default_schema, default_schema_default)
      @default_string_column_size = @opts[:default_string_column_size] || DEFAULT_STRING_COLUMN_SIZE
      @prepared_statements = {}
      @transactions = {}
      @identifier_input_method = nil
      @identifier_output_method = nil
      @quote_identifiers = nil
      @timezone = nil
      @dataset_class = dataset_class_default
      @cache_schema = typecast_value_boolean(@opts.fetch(:cache_schema, true))
      @dataset_modules = []
      self.sql_log_level = @opts[:sql_log_level] ? @opts[:sql_log_level].to_sym : :info
      @pool = ConnectionPool.get_pool(self, @opts)

      Sequel.synchronize{::Sequel::DATABASES.push(self)}
    end

    # If a transaction is not currently in process, yield to the block immediately.
    # Otherwise, add the block to the list of blocks to call after the currently
    # in progress transaction commits (and only if it commits).
    # Options:
    # :server :: The server/shard to use.
    def after_commit(opts={}, &block)
      raise Error, "must provide block to after_commit" unless block
      synchronize(opts[:server]) do |conn|
        if h = _trans(conn)
          raise Error, "cannot call after_commit in a prepared transaction" if h[:prepare]
          (h[:after_commit] ||= []) << block
        else
          yield
        end
      end
    end
    
    # If a transaction is not currently in progress, ignore the block.
    # Otherwise, add the block to the list of the blocks to call after the currently
    # in progress transaction rolls back (and only if it rolls back).
    # Options:
    # :server :: The server/shard to use.
    def after_rollback(opts={}, &block)
      raise Error, "must provide block to after_rollback" unless block
      synchronize(opts[:server]) do |conn|
        if h = _trans(conn)
          raise Error, "cannot call after_rollback in a prepared transaction" if h[:prepare]
          (h[:after_rollback] ||= []) << block
        end
      end
    end
    
    # Cast the given type to a literal type
    #
    #   DB.cast_type_literal(Float) # double precision
    #   DB.cast_type_literal(:foo) # foo
    def cast_type_literal(type)
      type_literal(:type=>type)
    end

    # Load an extension into the receiver.  In addition to requiring the extension file, this
    # also modifies the database to work with the extension (usually extending it with a
    # module defined in the extension file).  If no related extension file exists or the
    # extension does not have specific support for Database objects, an Error will be raised.
    # Returns self.
    def extension(*exts)
      Sequel.extension(*exts)
      exts.each do |ext|
        if pr = Sequel.synchronize{EXTENSIONS[ext]}
          pr.call(self)
        else
          raise(Error, "Extension #{ext} does not have specific support handling individual databases")
        end
      end
      self
    end

    # Convert the given timestamp from the application's timezone,
    # to the databases's timezone or the default database timezone if
    # the database does not have a timezone.
    def from_application_timestamp(v)
      Sequel.convert_output_timestamp(v, timezone)
    end

    # Whether the database uses a global namespace for the index.  If
    # false, the indexes are going to be namespaced per table.
    def global_index_namespace?
      true
    end

    # Return true if already in a transaction given the options,
    # false otherwise.  Respects the :server option for selecting
    # a shard.
    def in_transaction?(opts={})
      synchronize(opts[:server]){|conn| !!_trans(conn)}
    end

    # Returns a string representation of the database object including the
    # class name and connection URI and options used when connecting (if any).
    def inspect
      a = []
      a << uri.inspect if uri
      if (oo = opts[:orig_opts]) && !oo.empty?
        a << oo.inspect
      end
      "#<#{self.class}: #{a.join(' ')}>"
    end

    # Proxy the literal call to the dataset.
    #
    #   DB.literal(1) # 1
    #   DB.literal(:a) # a
    #   DB.literal('a') # 'a'
    def literal(v)
      schema_utility_dataset.literal(v)
    end

    # Synchronize access to the prepared statements cache.
    def prepared_statement(name)
      Sequel.synchronize{prepared_statements[name]}
    end

    # Default serial primary key options, used by the table creation
    # code.
    def serial_primary_key_options
      {:primary_key => true, :type => Integer, :auto_increment => true}
    end

    # Cache the prepared statement object at the given name.
    def set_prepared_statement(name, ps)
      Sequel.synchronize{prepared_statements[name] = ps}
    end

    # Whether the database supports CREATE TABLE IF NOT EXISTS syntax,
    # false by default.
    def supports_create_table_if_not_exists?
      false
    end

    # Whether the database supports deferrable constraints, false
    # by default as few databases do.
    def supports_deferrable_constraints?
      false
    end

    # Whether the database supports deferrable foreign key constraints,
    # false by default as few databases do.
    def supports_deferrable_foreign_key_constraints?
      supports_deferrable_constraints?
    end

    # Whether the database supports DROP TABLE IF EXISTS syntax,
    # default is the same as #supports_create_table_if_not_exists?.
    def supports_drop_table_if_exists?
      supports_create_table_if_not_exists?
    end

    # Whether the database and adapter support prepared transactions
    # (two-phase commit), false by default.
    def supports_prepared_transactions?
      false
    end

    # Whether the database and adapter support savepoints, false by default.
    def supports_savepoints?
      false
    end

    # Whether the database and adapter support savepoints inside prepared transactions
    # (two-phase commit), default is false.
    def supports_savepoints_in_prepared_transactions?
      supports_prepared_transactions? && supports_savepoints?
    end

    # Whether the database and adapter support transaction isolation levels, false by default.
    def supports_transaction_isolation_levels?
      false
    end

    # Whether DDL statements work correctly in transactions, false by default.
    def supports_transactional_ddl?
      false
    end

    # The timezone to use for this database, defaulting to <tt>Sequel.database_timezone</tt>.
    def timezone
      @timezone || Sequel.database_timezone
    end

    # Convert the given timestamp to the application's timezone,
    # from the databases's timezone or the default database timezone if
    # the database does not have a timezone.
    def to_application_timestamp(v)
      Sequel.convert_timestamp(v, timezone)
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
    
    # Returns the URI use to connect to the database.  If a URI
    # was not used when connecting, returns nil.
    def uri
      opts[:uri]
    end
    
    # Explicit alias of uri for easier subclassing.
    def url
      uri
    end
    
    private
    
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
    
    # Which transaction errors to translate, blank by default.
    def database_error_classes
      []
    end

    # Return true if exception represents a disconnect error, false otherwise.
    def disconnect_error?(exception, opts)
      opts[:disconnect]
    end
    
    # Convert the given exception to a DatabaseError, keeping message
    # and traceback.
    def raise_error(exception, opts={})
      if !opts[:classes] || Array(opts[:classes]).any?{|c| exception.is_a?(c)}
        raise Sequel.convert_exception_class(exception, disconnect_error?(exception, opts) ? DatabaseDisconnectError : DatabaseError)
      else
        raise exception
      end
    end
    
    # Typecast the value to an SQL::Blob
    def typecast_value_blob(value)
      value.is_a?(Sequel::SQL::Blob) ? value : Sequel::SQL::Blob.new(value)
    end

    # Typecast the value to true, false, or nil
    def typecast_value_boolean(value)
      case value
      when false, 0, "0", /\Af(alse)?\z/i, /\Ano?\z/i
        false
      else
        blank_object?(value) ? nil : true
      end
    end

    # Typecast the value to a Date
    def typecast_value_date(value)
      case value
      when DateTime, Time
        Date.new(value.year, value.month, value.day)
      when Date
        value
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
      Sequel.typecast_to_application_timestamp(value)
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

    # Used for checking/removing leading zeroes from strings so they don't get
    # interpreted as octal.
    LEADING_ZERO_RE = /\A0+(\d)/.freeze
    if RUBY_VERSION >= '1.9'
      # Typecast the value to an Integer
      def typecast_value_integer(value)
        (value.is_a?(String) && value =~ LEADING_ZERO_RE) ? Integer(value, 10) : Integer(value)
      end
    else
      # Replacement string when replacing leading zeroes.
      LEADING_ZERO_REP = "\\1".freeze 
      # Typecast the value to an Integer
      def typecast_value_integer(value)
        Integer(value.is_a?(String) ? value.sub(LEADING_ZERO_RE, LEADING_ZERO_REP) : value)
      end
    end

    # Typecast the value to a String
    def typecast_value_string(value)
      value.to_s
    end

    # Typecast the value to a Time
    def typecast_value_time(value)
      case value
      when Time
        if value.is_a?(SQLTime)
          value
        else
          # specifically check for nsec == 0 value to work around JRuby 1.6 ruby 1.9 mode bug
          SQLTime.create(value.hour, value.min, value.sec, (value.respond_to?(:nsec) && value.nsec != 0) ? value.nsec/1000.0 : value.usec)
        end
      when String
        Sequel.string_to_time(value)
      when Hash
        SQLTime.create(*[:hour, :minute, :second].map{|x| (value[x] || value[x.to_s]).to_i})
      else
        raise Sequel::InvalidValue, "invalid value for Time: #{value.inspect}"
      end
    end
  end
end
