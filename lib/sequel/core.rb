%w'bigdecimal date thread time uri'.each{|f| require f}

# Top level module for Sequel
#
# There are some class methods that are added via metaprogramming, one for
# each supported adapter.  For example:
#
#   DB = Sequel.sqlite # Memory database
#   DB = Sequel.sqlite('blog.db')
#   DB = Sequel.postgres('database_name', :user=>'user', 
#          :password=>'password', :host=>'host', :port=>5432, 
#          :max_connections=>10)
#
# If a block is given to these methods, it is passed the opened Database
# object, which is closed (disconnected) when the block exits, just
# like a block passed to connect.  For example:
#
#   Sequel.sqlite('blog.db'){|db| puts db[:users].count} 
#
# Sequel doesn't pay much attention to timezones by default, but you can set it
# handle timezones if you want.  There are three separate timezone settings:
#
# * application_timezone - The timezone you want the application to use.  This is
#   the timezone that incoming times from the database and typecasting are converted
#   to.
# * database_timezone - The timezone for storage in the database.  This is the
#   timezone to which Sequel will convert timestamps before literalizing them
#   for storage in the database.  It is also the timezone that Sequel will assume
#   database timestamp values are already in (if they don't include an offset).
# * typecast_timezone - The timezone that incoming data that Sequel needs to typecast
#   is assumed to be already in (if they don't include an offset).
#
# You can set also set all three timezones to the same value at once via
# Sequel.default_timezone=.
#
#   Sequel.application_timezone = :utc # or :local or nil
#   Sequel.database_timezone = :utc # or :local or nil
#   Sequel.typecast_timezone = :utc # or :local or nil
#   Sequel.default_timezone = :utc # or :local or nil
#
# The only timezone values that are supported by default are :utc (convert to UTC),
# :local (convert to local time), and nil (don't convert).  If you need to
# convert to a specific timezone, or need the timezones being used to change based
# on the environment (e.g. current user), you need to use an extension (and use
# DateTime as the datetime_class).
#
# You can set the SEQUEL_NO_CORE_EXTENSIONS constant or environment variable to have
# Sequel not extend the core classes.
module Sequel
  @convert_two_digit_years = true
  @datetime_class = Time
  @virtual_row_instance_eval = true
  @require_thread = nil
  
  # Mutex used to protect file loading
  @require_mutex = Mutex.new
  
  class << self
    # Sequel converts two digit years in Dates and DateTimes by default,
    # so 01/02/03 is interpreted at January 2nd, 2003, and 12/13/99 is interpreted
    # as December 13, 1999. You can override this to treat those dates as
    # January 2nd, 0003 and December 13, 0099, respectively, by setting this to false.
    attr_accessor :convert_two_digit_years

    # Sequel can use either Time or DateTime for times returned from the
    # database.  It defaults to Time.  To change it to DateTime, set this to DateTime.
    attr_accessor :datetime_class

    attr_accessor :virtual_row_instance_eval
    
    # Alias to the standard version of require
    alias k_require require

    private

    # Make thread safe requiring reentrant to prevent deadlocks.
    def check_requiring_thread
      t = Thread.current
      return(yield) if @require_thread == t
      @require_mutex.synchronize do
        begin
          @require_thread = t 
          yield
        ensure
          @require_thread = nil
        end
      end
    end
  end

  # Returns true if the passed object could be a specifier of conditions, false otherwise.
  # Currently, Sequel considers hashes and arrays of all two pairs as
  # condition specifiers.
  def self.condition_specifier?(obj)
    case obj
    when Hash
      true
    when Array
      !obj.empty? && obj.all?{|i| (Array === i) && (i.length == 2)}
    else
      false
    end
  end

  # Creates a new database object based on the supplied connection string
  # and optional arguments.  The specified scheme determines the database
  # class used, and the rest of the string specifies the connection options.
  # For example:
  #
  #   DB = Sequel.connect('sqlite:/') # Memory database
  #   DB = Sequel.connect('sqlite://blog.db') # ./blog.db
  #   DB = Sequel.connect('sqlite:///blog.db') # /blog.db
  #   DB = Sequel.connect('postgres://user:password@host:port/database_name')
  #   DB = Sequel.connect('sqlite:///blog.db', :max_connections=>10)
  #
  # If a block is given, it is passed the opened Database object, which is
  # closed when the block exits.  For example:
  #
  #   Sequel.connect('sqlite://blog.db'){|db| puts db[:users].count}  
  # 
  # For details, see the "Connecting to a Database" Guide.
  def self.connect(*args, &block)
    Database.connect(*args, &block)
  end
  
  # Convert the exception to the given class.  The given class should be
  # Sequel::Error or a subclass.  Returns an instance of klass with
  # the message and backtrace of exception.
  def self.convert_exception_class(exception, klass)
    return exception if exception.is_a?(klass)
    e = klass.new("#{exception.class}: #{exception.message}")
    e.wrapped_exception = exception
    e.set_backtrace(exception.backtrace)
    e
  end

  # Load all Sequel extensions given.  Only loads extensions included in this
  # release of Sequel, doesn't load external extensions.
  #
  #   Sequel.extension(:schema_dumper)
  #   Sequel.extension(:pagination, :query)
  def self.extension(*extensions)
    extensions.each{|e| tsk_require "sequel/extensions/#{e}"}
  end
  
  # Set the method to call on identifiers going into the database.  This affects
  # the literalization of identifiers by calling this method on them before they are input.
  # Sequel upcases identifiers in all SQL strings for most databases, so to turn that off:
  #
  #   Sequel.identifier_input_method = nil
  # 
  # to downcase instead:
  #
  #   Sequel.identifier_input_method = :downcase
  #
  # Other String instance methods work as well.
  def self.identifier_input_method=(value)
    Database.identifier_input_method = value
  end
  
  # Set the method to call on identifiers coming out of the database.  This affects
  # the literalization of identifiers by calling this method on them when they are
  # retrieved from the database.  Sequel downcases identifiers retrieved for most
  # databases, so to turn that off:
  #
  #   Sequel.identifier_output_method = nil
  # 
  # to upcase instead:
  #
  #   Sequel.identifier_output_method = :upcase
  #
  # Other String instance methods work as well.
  def self.identifier_output_method=(value)
    Database.identifier_output_method = value
  end
  
  # Set whether to quote identifiers for all databases by default. By default,
  # Sequel quotes identifiers in all SQL strings, so to turn that off:
  #
  #   Sequel.quote_identifiers = false
  def self.quote_identifiers=(value)
    Database.quote_identifiers = value
  end
  
  # Require all given files which should be in the same or a subdirectory of
  # this file.  If a subdir is given, assume all files are in that subdir.
  def self.require(files, subdir=nil)
    Array(files).each{|f| super("#{File.dirname(__FILE__)}/#{"#{subdir}/" if subdir}#{f}")}
  end
  
  # Set whether to set the single threaded mode for all databases by default. By default,
  # Sequel uses a threadsafe connection pool, which isn't as fast as the
  # single threaded connection pool.  If your program will only have one thread,
  # and speed is a priority, you may want to set this to true:
  #
  #   Sequel.single_threaded = true
  def self.single_threaded=(value)
    Database.single_threaded = value
  end

  # Converts the given string into a Date object.
  def self.string_to_date(s)
    begin
      Date.parse(s, Sequel.convert_two_digit_years)
    rescue => e
      raise convert_exception_class(e, InvalidValue)
    end
  end

  # Converts the given string into a Time or DateTime object, depending on the
  # value of Sequel.datetime_class.
  def self.string_to_datetime(s)
    begin
      if datetime_class == DateTime
        DateTime.parse(s, convert_two_digit_years)
      else
        datetime_class.parse(s)
      end
    rescue => e
      raise convert_exception_class(e, InvalidValue)
    end
  end

  # Converts the given string into a Time object.
  def self.string_to_time(s)
    begin
      Time.parse(s)
    rescue => e
      raise convert_exception_class(e, InvalidValue)
    end
  end

  # Same as Sequel.require, but wrapped in a mutex in order to be thread safe.
  def self.ts_require(*args)
    check_requiring_thread{require(*args)}
  end
  
  # Same as Kernel.require, but wrapped in a mutex in order to be thread safe.
  def self.tsk_require(*args)
    check_requiring_thread{k_require(*args)}
  end

  # If the supplied block takes a single argument,
  # yield a new SQL::VirtualRow instance to the block
  # argument.  Otherwise, evaluate the block in the context of a new
  # SQL::VirtualRow instance.
  def self.virtual_row(&block)
    vr = SQL::VirtualRow.new
    case block.arity
    when -1, 0
      vr.instance_eval(&block)
    else
      block.call(vr)
    end  
  end
  
  ### Private Class Methods ###

  # Helper method that the database adapter class methods that are added to Sequel via
  # metaprogramming use to parse arguments.
  def self.adapter_method(adapter, *args, &block) # :nodoc:
    raise(::Sequel::Error, "Wrong number of arguments, 0-2 arguments valid") if args.length > 2
    opts = {:adapter=>adapter.to_sym}
    opts[:database] = args.shift if args.length >= 1 && !(args[0].is_a?(Hash))
    if Hash === (arg = args[0])
      opts.merge!(arg)
    elsif !arg.nil?
      raise ::Sequel::Error, "Wrong format of arguments, either use (), (String), (Hash), or (String, Hash)"
    end
    connect(opts, &block)
  end

  # Method that adds a database adapter class method to Sequel that calls
  # Sequel.adapter_method.
  def self.def_adapter_method(*adapters) # :nodoc:
    adapters.each do |adapter|
      instance_eval("def #{adapter}(*args, &block); adapter_method('#{adapter}', *args, &block) end", __FILE__, __LINE__)
    end
  end

  private_class_method :adapter_method, :def_adapter_method
  
  require(%w"metaprogramming sql connection_pool exceptions dataset database timezones version")
  require(%w"schema_generator schema_methods schema_sql", 'database')
  require(%w"actions convenience features graph prepared_statements query sql", 'dataset')
  require('core_sql') if !defined?(::SEQUEL_NO_CORE_EXTENSIONS) && !ENV.has_key?('SEQUEL_NO_CORE_EXTENSIONS')

  # Add the database adapter class methods to Sequel via metaprogramming
  def_adapter_method(*Database::ADAPTERS)
end
