%w'bigdecimal bigdecimal/util date enumerator metaid thread time uri yaml'.each do |f|
  require f
end
%w"deprecated core_ext core_sql connection_pool exceptions pretty_table
  dataset migration schema database worker object_graph".each do |f|
  require "sequel_core/#{f}"
end

module Sequel #:nodoc:
  Deprecation.deprecation_message_stream = STDERR

  # call-seq:
  #   Sequel::Database.connect(conn_string)
  #   Sequel.connect(conn_string)
  #   Sequel.open(conn_string)
  #
  # Creates a new database object based on the supplied connection string.
  # The specified scheme determines the database class used, and the rest
  # of the string specifies the connection options. For example:
  #   DB = Sequel.open 'sqlite:///blog.db'
  def self.connect(*args, &block)
    Database.connect(*args, &block)
  end
  metaalias :open, :connect
  
  def self.single_threaded=(value)
    Database.single_threaded = value
  end
  
  ### Private Class Methods ###

  def self.adapter_method(adapter, *args, &block)
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

  def self.def_adapter_method(*adapters)
    adapters.each do |adapter|
      instance_eval("def #{adapter}(*args, &block); adapter_method('#{adapter}', *args, &block) end")
    end
  end
  metaprivate :adapter_method, :def_adapter_method
  
  def_adapter_method(*Database::ADAPTERS)
end
