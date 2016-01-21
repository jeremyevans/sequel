# frozen-string-literal: true

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

    # Empty exception regexp to class map, used by default if Sequel doesn't
    # have specific support for the database in use.
    DEFAULT_DATABASE_ERROR_REGEXPS = {}.freeze

    # Mapping of schema type symbols to class or arrays of classes for that
    # symbol.
    SCHEMA_TYPE_CLASSES = {:string=>String, :integer=>Integer, :date=>Date, :datetime=>[Time, DateTime].freeze,
      :time=>Sequel::SQLTime, :boolean=>[TrueClass, FalseClass].freeze, :float=>Float, :decimal=>BigDecimal,
      :blob=>Sequel::SQL::Blob}.freeze

    # Module to be included in shared adapters so that when the DatabaseMethods are
    # included in the database, the identifier mangling defaults are reset correctly.
    module ResetIdentifierMangling
      def extended(obj)
        obj.send(:reset_identifier_mangling)
      end
    end

    # Nested hook Proc; each new hook Proc just wraps the previous one.
    @initialize_hook = Proc.new {|db| }

    # Register a hook that will be run when a new Database is instantiated. It is
    # called with the new database handle.
    def self.after_initialize(&block)
      raise Error, "must provide block to after_initialize" unless block
      Sequel.synchronize do
        previous = @initialize_hook
        @initialize_hook = Proc.new do |db|
          previous.call(db)
          block.call(db)
        end
      end
    end

    # Apply an extension to all Database objects created in the future.
    def self.extension(*extensions)
      after_initialize{|db| db.extension(*extensions)}
    end

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

    # Run the after_initialize hook for the given +instance+.
    def self.run_after_initialize(instance)
      @initialize_hook.call(instance)
    end

    # Converts a uri to an options hash. These options are then passed
    # to a newly created database object. 
    def self.uri_to_options(uri)
      {
        :user => uri.user,
        :password => uri.password,
        :port => uri.port,
        :host => RUBY_VERSION < '1.9.3' ? uri.host : uri.hostname,
        :database => (m = /\/(.*)/.match(uri.path)) && (m[1])
      }
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
    # :default_string_column_size :: The default size of string columns, 255 by default.
    # :identifier_input_method :: A string method symbol to call on identifiers going into the database.
    # :identifier_output_method :: A string method symbol to call on identifiers coming from the database.
    # :logger :: A specific logger to use.
    # :loggers :: An array of loggers to use.
    # :preconnect :: Whether to automatically connect to the maximum number of servers.
    # :quote_identifiers :: Whether to quote identifiers.
    # :servers :: A hash specifying a server/shard specific options, keyed by shard symbol .
    # :single_threaded :: Whether to use a single-threaded connection pool.
    # :sql_log_level :: Method to use to log SQL to a logger, :info by default.
    #
    # All options given are also passed to the connection pool.
    def initialize(opts = OPTS, &block)
      @opts ||= opts
      @opts = connection_pool_default_options.merge(@opts)
      @loggers = Array(@opts[:logger]) + Array(@opts[:loggers])
      self.log_warn_duration = @opts[:log_warn_duration]
      block ||= proc{|server| connect(server)}
      @opts[:servers] = {} if @opts[:servers].is_a?(String)
      @sharded = !!@opts[:servers]
      @opts[:adapter_class] = self.class
      
      @opts[:single_threaded] = @single_threaded = typecast_value_boolean(@opts.fetch(:single_threaded, Database.single_threaded))
      @schemas = {}
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
      @symbol_literal_cache = {}
      @schema_type_classes = SCHEMA_TYPE_CLASSES.dup
      self.sql_log_level = @opts[:sql_log_level] ? @opts[:sql_log_level].to_sym : :info
      @pool = ConnectionPool.get_pool(self, @opts)

      reset_identifier_mangling
      adapter_initialize

      unless typecast_value_boolean(@opts[:keep_reference]) == false
        Sequel.synchronize{::Sequel::DATABASES.push(self)}
      end
      Sequel::Database.run_after_initialize(self)
      @pool.send(:preconnect) if typecast_value_boolean(@opts[:preconnect]) && @pool.respond_to?(:preconnect, true)
    end

    # If a transaction is not currently in process, yield to the block immediately.
    # Otherwise, add the block to the list of blocks to call after the currently
    # in progress transaction commits (and only if it commits).
    # Options:
    # :server :: The server/shard to use.
    def after_commit(opts=OPTS, &block)
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
    def after_rollback(opts=OPTS, &block)
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
          raise(Error, "Extension #{ext} does not have specific support handling individual databases (try: Sequel.extension #{ext.inspect})")
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

    # Return true if already in a transaction given the options,
    # false otherwise.  Respects the :server option for selecting
    # a shard.
    def in_transaction?(opts=OPTS)
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

    # Return the literalized version of the symbol if cached, or
    # nil if it is not cached.
    def literal_symbol(sym)
      Sequel.synchronize{@symbol_literal_cache[sym]}
    end

    # Set the cached value of the literal symbol.
    def literal_symbol_set(sym, lit)
      Sequel.synchronize{@symbol_literal_cache[sym] = lit}
    end

    # Synchronize access to the prepared statements cache.
    def prepared_statement(name)
      Sequel.synchronize{prepared_statements[name]}
    end

    # Proxy the quote_identifier method to the dataset,
    # useful for quoting unqualified identifiers for use
    # outside of datasets.
    def quote_identifier(v)
      schema_utility_dataset.quote_identifier(v)
    end

    # Return ruby class or array of classes for the given type symbol.
    def schema_type_class(type)
      @schema_type_classes[type]
    end
    
    # Default serial primary key options, used by the table creation
    # code.
    def serial_primary_key_options
      {:primary_key => true, :type => Integer, :auto_increment => true}
    end

    # Cache the prepared statement object at the given name.
    def set_prepared_statement(name, ps)
      ps.prepared_sql
      Sequel.synchronize{prepared_statements[name] = ps}
    end

    # Whether this database instance uses multiple servers, either for sharding
    # or for master/slave.
    def sharded?
      @sharded
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
    
    # Per adapter initialization method, empty by default.
    def adapter_initialize
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

    # Which transaction errors to translate, blank by default.
    def database_error_classes
      []
    end

    # An enumerable yielding pairs of regexps and exception classes, used
    # to match against underlying driver exception messages in
    # order to raise a more specific Sequel::DatabaseError subclass.
    def database_error_regexps
      DEFAULT_DATABASE_ERROR_REGEXPS
    end

    # Return the Sequel::DatabaseError subclass to wrap the given
    # exception in.
    def database_error_class(exception, opts)
      database_specific_error_class(exception, opts) || DatabaseError
    end
    
    # Return the SQLState for the given exception, if one can be
    # determined
    def database_exception_sqlstate(exception, opts)
      nil
    end

    # Return a specific Sequel::DatabaseError exception class if
    # one is appropriate for the underlying exception,
    # or nil if there is no specific exception class.
    def database_specific_error_class(exception, opts)
      return DatabaseDisconnectError if disconnect_error?(exception, opts)

      if sqlstate = database_exception_sqlstate(exception, opts)
        if klass = database_specific_error_class_from_sqlstate(sqlstate)
          return klass
        end
      else
        database_error_regexps.each do |regexp, klss|
          return klss if exception.message =~ regexp
        end
      end

      nil
    end
    
    NOT_NULL_CONSTRAINT_SQLSTATES = %w'23502'.freeze.each(&:freeze)
    FOREIGN_KEY_CONSTRAINT_SQLSTATES = %w'23503 23506 23504'.freeze.each(&:freeze)
    UNIQUE_CONSTRAINT_SQLSTATES = %w'23505'.freeze.each(&:freeze)
    CHECK_CONSTRAINT_SQLSTATES = %w'23513 23514'.freeze.each(&:freeze)
    SERIALIZATION_CONSTRAINT_SQLSTATES = %w'40001'.freeze.each(&:freeze)
    # Given the SQLState, return the appropriate DatabaseError subclass.
    def database_specific_error_class_from_sqlstate(sqlstate)
      case sqlstate
      when *NOT_NULL_CONSTRAINT_SQLSTATES
        NotNullConstraintViolation
      when *FOREIGN_KEY_CONSTRAINT_SQLSTATES
        ForeignKeyConstraintViolation
      when *UNIQUE_CONSTRAINT_SQLSTATES
        UniqueConstraintViolation
      when *CHECK_CONSTRAINT_SQLSTATES
        CheckConstraintViolation
      when *SERIALIZATION_CONSTRAINT_SQLSTATES
        SerializationFailure
      end
    end
    
    # Return true if exception represents a disconnect error, false otherwise.
    def disconnect_error?(exception, opts)
      opts[:disconnect]
    end
    
    # Convert the given exception to an appropriate Sequel::DatabaseError
    # subclass, keeping message and traceback.
    def raise_error(exception, opts=OPTS)
      if !opts[:classes] || Array(opts[:classes]).any?{|c| exception.is_a?(c)}
        raise Sequel.convert_exception_class(exception, database_error_class(exception, opts))
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
      when Numeric
        BigDecimal.new(value.to_s)
      when String
        d = BigDecimal.new(value)
        if d.zero?
          # BigDecimal parsing is loose by default, returning a 0 value for
          # invalid input.  If a zero value is received, use Float to check
          # for validity.
          begin
            Float(value)
          rescue ArgumentError
            raise InvalidValue, "invalid value for BigDecimal: #{value.inspect}"
          end
        end
        d
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
    # :nocov:
      # Replacement string when replacing leading zeroes.
      LEADING_ZERO_REP = "\\1".freeze 
      # Typecast the value to an Integer
      def typecast_value_integer(value)
        Integer(value.is_a?(String) ? value.sub(LEADING_ZERO_RE, LEADING_ZERO_REP) : value)
      end
    # :nocov:
    end

    # Typecast the value to a String
    def typecast_value_string(value)
      case value
      when Hash, Array
        raise Sequel::InvalidValue, "invalid value for String: #{value.inspect}"
      else
        value.to_s
      end
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
