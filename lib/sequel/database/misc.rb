module Sequel
  class Database
    # ---------------------
    # :section: Miscellaneous methods
    # These methods don't fit neatly into another category.
    # ---------------------

    # Converts a uri to an options hash. These options are then passed
    # to a newly created database object. 
    def self.uri_to_options(uri) # :nodoc:
      { :user => uri.user,
        :password => uri.password,
        :host => uri.host,
        :port => uri.port,
        :database => (m = /\/(.*)/.match(uri.path)) && (m[1]) }
    end
    private_class_method :uri_to_options
    
    # The options hash for this database
    attr_reader :opts
    
    # Constructs a new instance of a database connection with the specified
    # options hash.
    #
    # Accepts the following options:
    # :default_schema :: The default schema to use, should generally be nil
    # :disconnection_proc :: A proc used to disconnect the connection
    # :identifier_input_method :: A string method symbol to call on identifiers going into the database
    # :identifier_output_method :: A string method symbol to call on identifiers coming from the database
    # :logger :: A specific logger to use
    # :loggers :: An array of loggers to use
    # :quote_identifiers :: Whether to quote identifiers
    # :servers :: A hash specifying a server/shard specific options, keyed by shard symbol 
    # :single_threaded :: Whether to use a single-threaded connection pool
    #
    # All options given are also passed to the connection pool.  If a block
    # is given, it is used as the connection_proc for the ConnectionPool.
    def initialize(opts = {}, &block)
      @opts ||= opts
      @opts = connection_pool_default_options.merge(@opts)
      @loggers = Array(@opts[:logger]) + Array(@opts[:loggers])
      self.log_warn_duration = @opts[:log_warn_duration]
      @opts[:disconnection_proc] ||= proc{|conn| disconnect_connection(conn)}
      block ||= proc{|server| connect(server)}
      @opts[:servers] = {} if @opts[:servers].is_a?(String)
      
      @opts[:single_threaded] = @single_threaded = typecast_value_boolean(@opts.fetch(:single_threaded, @@single_threaded))
      @schemas = {}
      @default_schema = @opts.fetch(:default_schema, default_schema_default)
      @prepared_statements = {}
      @transactions = []
      @identifier_input_method = nil
      @identifier_output_method = nil
      @quote_identifiers = nil
      @pool = ConnectionPool.get_pool(@opts, &block)

      ::Sequel::DATABASES.push(self)
    end
    
    # Cast the given type to a literal type
    #
    #   DB.cast_type_literal(Float) # double precision
    #   DB.cast_type_literal(:foo) # foo
    def cast_type_literal(type)
      type_literal(:type=>type)
    end

    # Returns a string representation of the database object including the
    # class name and the connection URI (or the opts if the URI
    # cannot be constructed).
    def inspect
      "#<#{self.class}: #{(uri rescue opts).inspect}>" 
    end

    # Proxy the literal call to the dataset.
    #
    #   DB.literal(1) # 1
    #   DB.literal(:a) # a
    #   DB.literal('a') # 'a'
    def literal(v)
      schema_utility_dataset.literal(v)
    end

    # Default serial primary key options, used by the table creation
    # code.
    def serial_primary_key_options
      {:primary_key => true, :type => Integer, :auto_increment => true}
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

    # Whether the database and adapter support transaction isolation levels, false by default.
    def supports_transaction_isolation_levels?
      false
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
    
    # Returns the URI identifying the database, which may not be the
    # same as the URI used when connecting.
    # This method can raise an error if the database used options
    # instead of a connection string, and will not include uri
    # parameters.
    #
    #   Sequel.connect('postgres://localhost/db?user=billg').url
    #   # => "postgres://billg@localhost/db"
    def uri
      uri = URI::Generic.new(
        adapter_scheme.to_s,
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
    
    # Convert the given exception to a DatabaseError, keeping message
    # and traceback.
    def raise_error(exception, opts={})
      if !opts[:classes] || Array(opts[:classes]).any?{|c| exception.is_a?(c)}
        raise Sequel.convert_exception_class(exception, opts[:disconnect] ? DatabaseDisconnectError : DatabaseError)
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
