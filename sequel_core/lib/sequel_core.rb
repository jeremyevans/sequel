%w'bigdecimal bigdecimal/util date enumerator thread time uri yaml'.each do |f|
  require f
end
%w"core_ext sql core_sql connection_pool exceptions pretty_table
  dataset migration schema database worker object_graph deprecated".each do |f|
  require "sequel_core/#{f}"
end

# Top level module for Sequel
#
# There are some class methods that are added via metaprogramming, one for
# each supported adapter.  For example:
#
#   DB = Sequel.sqlite # Memory database
#   DB = Sequel.sqlite('blog.db')
#   DB = Sequel.postgres('database_name', :user=>'user', \
#          :password=>'password', :host=>'host', :port=>5432, \
#          :max_connections=>10)
#
# If a block is given to these methods, it is passed the opened Database
# object, which is closed (disconnected) when the block exits.  For example:
#
#   Sequel.sqlite('blog.db'){|db| puts db.users.count}  
#
# Sequel can use either Time or DateTime for times returned from the
# database.  It defaults to Time.  To change it to DateTime, use:
#
#   Sequel.datetime_class = DateTime
#
# Sequel can either use ParseTree for block filters (deprecated but works),
# or it can use the block filter syntax inside block filters (which will
# be the only behavior allowed in Sequel 2.2). To set it not to use
# ParseTree filters:
#
#   Sequel.use_parse_tree = false
module Sequel
  @datetime_class = Time
  @use_parse_tree = !defined?(SEQUEL_NO_PARSE_TREE)

  metaattr_accessor :datetime_class
  metaattr_accessor :use_parse_tree

  Deprecation.deprecation_message_stream = $stderr

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
  #   Sequel.connect('sqlite://blog.db'){|db| puts db.users.count}  
  #
  # This is also aliased as Sequel.open.
  def self.connect(*args, &block)
    Database.connect(*args, &block)
  end
  metaalias :open, :connect
  
  # Set whether to quote identifiers for all databases by default. By default,
  # Sequel quotes identifiers in all SQL strings, so to turn that off:
  #
  #   Sequel.quote_identifiers = false
  def self.quote_identifiers=(value)
    Database.quote_identifiers = value
  end
  
  # Set whether to set the single threaded mode for all databases by default. By default,
  # Sequel uses a threadsafe connection pool, which isn't as fast as the
  # single threaded connection pool.  If your program will only have one thread,
  # and speed is a priority, you may want to set this to true:
  #
  #   Sequel.single_threaded = true
  #
  # Note that some database adapters (e.g. MySQL) have issues with single threaded mode if
  # you try to perform more than one query simultaneously.  For example, the
  # following code will not work well in single threaded mode on MySQL:
  #
  #   DB[:items].each{|i| DB[:nodes].filter(:item_id=>i[:id]).each{|n| puts "#{i} #{n}"}}
  #
  # Basically, you can't issue another query inside a call to Dataset#each in single
  # threaded mode.  There is a fairly easy fix, just use Dataset#all inside
  # Dataset#each for the outer query:
  #
  #   DB[:items].all{|i| DB[:nodes].filter(:item_id=>i[:id]).each{|n| puts "#{i} #{n}"}}
  #
  # Dataset#all gets all of the returned objects before calling the block, so the query
  # isn't left open. Some of the adapters do this internally, and thus don't have a
  # problem issuing queries inside of Dataset#each.
  def self.single_threaded=(value)
    Database.single_threaded = value
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
  
  # Add the database adapter class methods to Sequel via metaprogramming
  def_adapter_method(*Database::ADAPTERS)
end
