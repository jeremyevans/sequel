%w'bigdecimal date thread time uri'.each{|f| require f}

# Top level module for Sequel
#
# There are some module methods that are added via metaprogramming, one for
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
# Sequel currently adds methods to the Array, Hash, String and Symbol classes by
# default.  You can either require 'sequel/no_core_ext' or set the
# +SEQUEL_NO_CORE_EXTENSIONS+ constant or environment variable before requiring
# sequel to have Sequel not add methods to those classes.
#
# For a more expanded introduction, see the {README}[link:files/README_rdoc.html].
# For a quicker introduction, see the {cheat sheet}[link:files/doc/cheat_sheet_rdoc.html].
module Sequel
  @convert_two_digit_years = true
  @datetime_class = Time
  @empty_array_handle_nulls = true

  # Whether Sequel is being run in single threaded mode
  @single_threaded = false

  class << self
    # Sequel converts two digit years in <tt>Date</tt>s and <tt>DateTime</tt>s by default,
    # so 01/02/03 is interpreted at January 2nd, 2003, and 12/13/99 is interpreted
    # as December 13, 1999. You can override this to treat those dates as
    # January 2nd, 0003 and December 13, 0099, respectively, by:
    #
    #   Sequel.convert_two_digit_years = false
    attr_accessor :convert_two_digit_years

    # Sequel can use either +Time+ or +DateTime+ for times returned from the
    # database.  It defaults to +Time+.  To change it to +DateTime+:
    #
    #   Sequel.datetime_class = DateTime
    #
    # For ruby versions less than 1.9.2, +Time+ has a limited range (1901 to
    # 2038), so if you use datetimes out of that range, you need to switch
    # to +DateTime+.  Also, before 1.9.2, +Time+ can only handle local and UTC
    # times, not other timezones.  Note that +Time+ and +DateTime+ objects
    # have a different API, and in cases where they implement the same methods,
    # they often implement them differently (e.g. + using seconds on +Time+ and
    # days on +DateTime+).
    attr_accessor :datetime_class

    # Sets whether or not to attempt to handle NULL values correctly when given
    # an empty array.  By default:
    #
    #   DB[:a].filter(:b=>[])
    #   # SELECT * FROM a WHERE (b != b)
    #   DB[:a].exclude(:b=>[])
    #   # SELECT * FROM a WHERE (b = b)
    #
    # However, some databases (e.g. MySQL) will perform very poorly
    # with this type of query.  You can set this to false to get the
    # following behavior:
    #
    #   DB[:a].filter(:b=>[])
    #   # SELECT * FROM a WHERE 1 = 0
    #   DB[:a].exclude(:b=>[])
    #   # SELECT * FROM a WHERE 1 = 1
    # 
    # This may not handle NULLs correctly, but can be much faster on
    # some databases.
    attr_reader :empty_array_handle_nulls

    def empty_array_handle_nulls=(v)
      Sequel::Deprecation.deprecate('Sequel.empty_array_handle_nulls=', 'Please switch to loading the empty_array_ignore_nulls plugin if you wish empty array handling to ignore nulls')
      @empty_array_handle_nulls = v
    end

    # REMOVE40
    def virtual_row_instance_eval
      Sequel::Deprecation.deprecate('Sequel.virtual_row_instance_eval', 'It has no effect, so you can safely stop calling it.')
    end
    def virtual_row_instance_eval=(v)
      Sequel::Deprecation.deprecate('Sequel.virtual_row_instance_eval=', 'It has no effect, so you can safely stop calling it.')
    end
    def k_require(*a)
      Sequel::Deprecation.deprecate('Sequel.k_require', 'Please switch to Kernel.require')
      Kernel.require(*a)
    end

    private

    # Make thread safe requiring reentrant to prevent deadlocks.
    def check_requiring_thread
      Sequel::Deprecation.deprecate('Sequel.check_requiring_thread', 'It has no effect, so you can safely stop calling it.')
    end
  end

  # Returns true if the passed object could be a specifier of conditions, false otherwise.
  # Currently, Sequel considers hashes and arrays of two element arrays as
  # condition specifiers.
  #
  #   Sequel.condition_specifier?({}) # => true
  #   Sequel.condition_specifier?([[1, 2]]) # => true
  #   Sequel.condition_specifier?([]) # => false
  #   Sequel.condition_specifier?([1]) # => false
  #   Sequel.condition_specifier?(1) # => false
  def self.condition_specifier?(obj)
    case obj
    when Hash
      true
    when Array
      !obj.empty? && !obj.is_a?(SQL::ValueList) && obj.all?{|i| i.is_a?(Array) && (i.length == 2)}
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
  # If a block is given, it is passed the opened +Database+ object, which is
  # closed when the block exits.  For example:
  #
  #   Sequel.connect('sqlite://blog.db'){|db| puts db[:users].count}  
  # 
  # For details, see the {"Connecting to a Database" guide}[link:files/doc/opening_databases_rdoc.html].
  # To set up a master/slave or sharded database connection, see the {"Master/Slave Databases and Sharding" guide}[link:files/doc/sharding_rdoc.html].
  def self.connect(*args, &block)
    Database.connect(*args, &block)
  end

  # Assume the core extensions are not loaded by default, if the core_extensions
  # extension is loaded, this will be overridden.
  def self.core_extensions?
    false
  end

  # Convert the +exception+ to the given class.  The given class should be
  # <tt>Sequel::Error</tt> or a subclass.  Returns an instance of +klass+ with
  # the message and backtrace of +exception+.
  def self.convert_exception_class(exception, klass)
    return exception if exception.is_a?(klass)
    e = klass.new("#{exception.class}: #{exception.message}")
    e.wrapped_exception = exception
    e.set_backtrace(exception.backtrace)
    e
  end

  # Load all Sequel extensions given.  Extensions are just files that exist under
  # <tt>sequel/extensions</tt> in the load path, and are just required.  Generally,
  # extensions modify the behavior of +Database+ and/or +Dataset+, but Sequel ships
  # with some extensions that modify other classes that exist for backwards compatibility.
  # In some cases, requiring an extension modifies classes directly, and in others,
  # it just loads a module that you can extend other classes with.  Consult the documentation
  # for each extension you plan on using for usage.
  #
  #   Sequel.extension(:schema_dumper)
  #   Sequel.extension(:pagination, :query)
  def self.extension(*extensions)
    extensions.each{|e| Kernel.require "sequel/extensions/#{e}"}
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

  # The exception classed raised if there is an error parsing JSON.
  # This can be overridden to use an alternative json implementation.
  def self.json_parser_error_class
    JSON::ParserError
  end

  # Convert given object to json and return the result.
  # This can be overridden to use an alternative json implementation.
  def self.object_to_json(obj, *args)
    obj.to_json(*args)
  end

  # Parse the string as JSON and return the result.
  # This can be overridden to use an alternative json implementation.
  def self.parse_json(json)
    JSON.parse(json, :create_additions=>false)
  end

  # Set whether to quote identifiers for all databases by default. By default,
  # Sequel quotes identifiers in all SQL strings, so to turn that off:
  #
  #   Sequel.quote_identifiers = false
  def self.quote_identifiers=(value)
    Database.quote_identifiers = value
  end

  # Convert each item in the array to the correct type, handling multi-dimensional
  # arrays.  For each element in the array or subarrays, call the converter,
  # unless the value is nil.
  def self.recursive_map(array, converter)
    array.map do |i|
      if i.is_a?(Array)
        recursive_map(i, converter)
      elsif i
        converter.call(i)
      end
    end
  end

  # Require all given +files+ which should be in the same or a subdirectory of
  # this file.  If a +subdir+ is given, assume all +files+ are in that subdir.
  # This is used to ensure that the files loaded are from the same version of
  # Sequel as this file.
  def self.require(files, subdir=nil)
    Array(files).each{|f| super("#{File.dirname(__FILE__).untaint}/#{"#{subdir}/" if subdir}#{f}")}
  end

  # Set whether Sequel is being used in single threaded mode. By default,
  # Sequel uses a thread-safe connection pool, which isn't as fast as the
  # single threaded connection pool, and also has some additional thread
  # safety checks.  If your program will only have one thread,
  # and speed is a priority, you should set this to true:
  #
  #   Sequel.single_threaded = true
  def self.single_threaded=(value)
    @single_threaded = value
    Database.single_threaded = value
  end

  COLUMN_REF_RE1 = /\A((?:(?!__).)+)__((?:(?!___).)+)___(.+)\z/.freeze
  COLUMN_REF_RE2 = /\A((?:(?!___).)+)___(.+)\z/.freeze
  COLUMN_REF_RE3 = /\A((?:(?!__).)+)__(.+)\z/.freeze

  # Splits the symbol into three parts.  Each part will
  # either be a string or nil.
  #
  # For columns, these parts are the table, column, and alias.
  # For tables, these parts are the schema, table, and alias.
  def self.split_symbol(sym)
    case s = sym.to_s
    when COLUMN_REF_RE1
      [$1, $2, $3]
    when COLUMN_REF_RE2
      [nil, $1, $2]
    when COLUMN_REF_RE3
      [$1, $2, nil]
    else
      [nil, s, nil]
    end
  end

  # Converts the given +string+ into a +Date+ object.
  #
  #   Sequel.string_to_date('2010-09-10') # Date.civil(2010, 09, 10)
  def self.string_to_date(string)
    begin
      Date.parse(string, Sequel.convert_two_digit_years)
    rescue => e
      raise convert_exception_class(e, InvalidValue)
    end
  end

  # Converts the given +string+ into a +Time+ or +DateTime+ object, depending on the
  # value of <tt>Sequel.datetime_class</tt>.
  #
  #   Sequel.string_to_datetime('2010-09-10 10:20:30') # Time.local(2010, 09, 10, 10, 20, 30)
  def self.string_to_datetime(string)
    begin
      if datetime_class == DateTime
        DateTime.parse(string, convert_two_digit_years)
      else
        datetime_class.parse(string)
      end
    rescue => e
      raise convert_exception_class(e, InvalidValue)
    end
  end

  # Converts the given +string+ into a <tt>Sequel::SQLTime</tt> object.
  #
  #   v = Sequel.string_to_time('10:20:30') # Sequel::SQLTime.parse('10:20:30')
  #   DB.literal(v) # => '10:20:30'
  def self.string_to_time(string)
    begin
      SQLTime.parse(string)
    rescue => e
      raise convert_exception_class(e, InvalidValue)
    end
  end

  if defined?(RUBY_ENGINE) && RUBY_ENGINE != 'ruby'
  # :nocov:
    # Mutex used to protect mutable data structures
    @data_mutex = Mutex.new

    # Unless in single threaded mode, protects access to any mutable
    # global data structure in Sequel.
    # Uses a non-reentrant mutex, so calling code should be careful.
    def self.synchronize(&block)
      @single_threaded ? yield : @data_mutex.synchronize(&block)
    end
  # :nocov:
  else
    # Yield directly to the block.  You don't need to synchronize
    # access on MRI because the GVL makes certain methods atomic.
    def self.synchronize
      yield
    end
  end

  # Uses a transaction on all given databases with the given options. This:
  #
  #   Sequel.transaction([DB1, DB2, DB3]){...}
  #
  # is equivalent to:
  #
  #   DB1.transaction do
  #     DB2.transaction do
  #       DB3.transaction do
  #         ...
  #       end
  #     end
  #   end
  #
  # except that if Sequel::Rollback is raised by the block, the transaction is
  # rolled back on all databases instead of just the last one.
  #
  # Note that this method cannot guarantee that all databases will commit or
  # rollback.  For example, if DB3 commits but attempting to commit on DB2
  # fails (maybe because foreign key checks are deferred), there is no way
  # to uncommit the changes on DB3.  For that kind of support, you need to
  # have two-phase commit/prepared transactions (which Sequel supports on
  # some databases).
  def self.transaction(dbs, opts={}, &block)
    unless opts[:rollback]
      rescue_rollback = true
      opts = opts.merge(:rollback=>:reraise)
    end
    pr = dbs.reverse.inject(block){|bl, db| proc{db.transaction(opts, &bl)}}
    if rescue_rollback
      begin
        pr.call
      rescue Sequel::Rollback
        nil
      end
    else
      pr.call
    end
  end

  # REMOVE40
  def self.ts_require(*args)
    Sequel::Deprecation.deprecate('Sequel.ts_require', 'Please switch to Sequel.require')
    require(*args)
  end
  def self.tsk_require(*args)
    Sequel::Deprecation.deprecate('Sequel.tsk_require', 'Please switch to Kernel.require')
    Kernel.require(*args)
  end

  # If the supplied block takes a single argument,
  # yield an <tt>SQL::VirtualRow</tt> instance to the block
  # argument.  Otherwise, evaluate the block in the context of a
  # <tt>SQL::VirtualRow</tt> instance.
  #
  #   Sequel.virtual_row{a} # Sequel::SQL::Identifier.new(:a)
  #   Sequel.virtual_row{|o| o.a{}} # Sequel::SQL::Function.new(:a)
  def self.virtual_row(&block)
    vr = VIRTUAL_ROW
    case block.arity
    when -1, 0
      vr.instance_exec(&block)
    else
      block.call(vr)
    end  
  end

  ### Private Class Methods ###

  # Helper method that the database adapter class methods that are added to Sequel via
  # metaprogramming use to parse arguments.
  def self.adapter_method(adapter, *args, &block)
    options = args.last.is_a?(Hash) ? args.pop : {}
    opts = {:adapter => adapter.to_sym}
    opts[:database] = args.shift if args.first.is_a?(String)
    if args.any?
      raise ::Sequel::Error, "Wrong format of arguments, either use (), (String), (Hash), or (String, Hash)"
    end

    connect(opts.merge(options), &block)
  end

  # Method that adds a database adapter class method to Sequel that calls
  # Sequel.adapter_method.
  #
  # Do not call this method with untrusted input, as that can result in
  # arbitrary code execution.
  def self.def_adapter_method(*adapters) # :nodoc:
    adapters.each do |adapter|
      instance_eval("def #{adapter}(*args, &block); adapter_method('#{adapter}', *args, &block) end", __FILE__, __LINE__)
    end
  end

  private_class_method :adapter_method, :def_adapter_method

  require(%w"deprecated sql connection_pool exceptions dataset database timezones ast_transformer version")
  if !defined?(::SEQUEL_NO_CORE_EXTENSIONS) && !ENV.has_key?('SEQUEL_NO_CORE_EXTENSIONS')
  # :nocov:
    require(:deprecated_core_extensions)
  # :nocov:
  end

  # Add the database adapter class methods to Sequel via metaprogramming
  def_adapter_method(*Database::ADAPTERS)
end
