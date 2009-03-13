%w'bigdecimal bigdecimal/util date enumerator thread time uri yaml'.each do |f|
  require f
end

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
# object, which is closed (disconnected) when the block exits.  For example:
#
#   Sequel.sqlite('blog.db'){|db| puts db.users.count}  
#
# Sequel converts the column type tinyint to a boolean by default,
# you can override the conversion to use tinyint as an integer:
#
#   Sequel.convert_tinyint_to_bool = false
#
# Sequel converts two digit years in Dates and DateTimes by default,
# so 01/02/03 is interpreted at January 2nd, 2003, and 12/13/99 is interpreted
# as December 13, 1999.. You can override this # to treat those dates as
# January 2nd, 0003 and December 13, 0099, respectively, by setting: 
#
#   Sequel.convert_two_digit_years = false
#
# Sequel can use either Time or DateTime for times returned from the
# database.  It defaults to Time.  To change it to DateTime, use:
#
#   Sequel.datetime_class = DateTime
#
# Sequel currently does not use instance_eval for virtual row blocks by default
# (e.g. the block passed to Dataset#filter, #select, #order and other similar
# methods).  If you want to use instance_eval for these blocks, don't have any
# block arguments, and set:
#
#   Sequel.virtual_row_instance_eval = true
#
# When this is set, you can do:
#
#   dataset.filter{|o| o.column > 0} # no instance_eval
#   dataset.filter{column > 0}       # instance eval
#
# When the virtual_row_instance_eval is false, using a virtual row block without a block
# argument will generate a deprecation message.
#
# The option to not use instance_eval for a block with no arguments will be removed in a future version.
# If you have any virtual row blocks that you don't want to use instance_eval for,
# make sure the blocks have block arguments.
module Sequel
  @convert_tinyint_to_bool = true
  @convert_two_digit_years = true
  @datetime_class = Time
  @virtual_row_instance_eval = false
  
  class << self
    attr_accessor :convert_tinyint_to_bool, :convert_two_digit_years, :datetime_class, :virtual_row_instance_eval
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
  def self.connect(*args, &block)
    Database.connect(*args, &block)
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
  # Other string methods work as well.
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
  # Other string methods work as well.
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
  # this file
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

  # Converts a string into a Date object.
  def self.string_to_date(s)
    begin
      Date.parse(s, Sequel.convert_two_digit_years)
    rescue => e
      raise Error::InvalidValue, "Invalid Date value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a Time or DateTime object, depending on the
  # value of Sequel.datetime_class.
  def self.string_to_datetime(s)
    begin
      if datetime_class == DateTime
        DateTime.parse(s, convert_two_digit_years)
      else
        datetime_class.parse(s)
      end
    rescue => e
      raise Error::InvalidValue, "Invalid #{datetime_class} value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a Time object.
  def self.string_to_time(s)
    begin
      Time.parse(s)
    rescue => e
      raise Error::InvalidValue, "Invalid Time value '#{self}' (#{e.message})"
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
      instance_eval("def #{adapter}(*args, &block); adapter_method('#{adapter}', *args, &block) end")
    end
  end

  private_class_method :adapter_method, :def_adapter_method
  
  require(%w"metaprogramming core_ext sql core_sql connection_pool exceptions dataset migration database object_graph version deprecated")
  require(%w"schema_generator schema_methods schema_sql", 'database')
  require(%w"convenience prepared_statements sql", 'dataset')

  # Add the database adapter class methods to Sequel via metaprogramming
  def_adapter_method(*Database::ADAPTERS)
end
