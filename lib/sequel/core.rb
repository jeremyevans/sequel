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
# sequel to have # Sequel not add methods to those classes.
#
# For a more expanded introduction, see the {README}[link:files/README_rdoc.html].
# For a quicker introduction, see the {cheat sheet}[link:files/doc/cheat_sheet_rdoc.html].
module Sequel
  @convert_two_digit_years = true
  @datetime_class = Time
  @empty_array_handle_nulls = true
  @virtual_row_instance_eval = true
  @require_thread = nil
  
  # Mutex used to protect file loading/requireing
  @require_mutex = Mutex.new
  
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
    attr_accessor :empty_array_handle_nulls

    # For backwards compatibility, has no effect.
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
      !obj.empty? && !obj.is_a?(SQL::ValueList) && obj.all?{|i| (Array === i) && (i.length == 2)}
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

  if !defined?(::SEQUEL_NO_CORE_EXTENSIONS) && !ENV.has_key?('SEQUEL_NO_CORE_EXTENSIONS')
    # Whether the core extensions are enabled.  The core extensions are enabled by
    # default for backwards compatibility, but can be disabled using the SEQUEL_NO_CORE_EXTENSIONS
    # constant or environment variable.
    def self.core_extensions?
      # We override this method to return true inside the core_extensions.rb file,
      # but we also set it here because that file is not loaded until most of Sequel
      # is finished loading, and parts of Sequel check whether the core extensions
      # are loaded.
      true
    end
  else
    def self.core_extensions?
      false
    end
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
  
  # Require all given +files+ which should be in the same or a subdirectory of
  # this file.  If a +subdir+ is given, assume all +files+ are in that subdir.
  # This is used to ensure that the files loaded are from the same version of
  # Sequel as this file.
  def self.require(files, subdir=nil)
    Array(files).each{|f| super("#{File.dirname(__FILE__).untaint}/#{"#{subdir}/" if subdir}#{f}")}
  end
  
  # Set whether to set the single threaded mode for all databases by default. By default,
  # Sequel uses a thread-safe connection pool, which isn't as fast as the
  # single threaded connection pool.  If your program will only have one thread,
  # and speed is a priority, you may want to set this to true:
  #
  #   Sequel.single_threaded = true
  def self.single_threaded=(value)
    Database.single_threaded = value
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
      rescue Sequel::Rollback => e
        nil
      end
    else
      pr.call
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
  # yield a new <tt>SQL::VirtualRow</tt> instance to the block
  # argument.  Otherwise, evaluate the block in the context of a new
  # <tt>SQL::VirtualRow</tt> instance.
  #
  #   Sequel.virtual_row{a} # Sequel::SQL::Identifier.new(:a)
  #   Sequel.virtual_row{|o| o.a{}} # Sequel::SQL::Function.new(:a)
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
  
  require(%w"metaprogramming sql connection_pool exceptions dataset database timezones ast_transformer version")
  extension(:core_extensions) if Sequel.core_extensions?

  # Add the database adapter class methods to Sequel via metaprogramming
  def_adapter_method(*Database::ADAPTERS)
end
