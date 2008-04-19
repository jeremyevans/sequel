require "metaid"
require "bigdecimal"
require "bigdecimal/util"

files = %w[
  deprecated core_ext core_sql connection_pool exceptions pretty_table
  dataset migration schema database worker object_graph
]
dir = File.join(File.dirname(__FILE__), "sequel_core")
files.each {|f| require(File.join(dir, f))}

module Sequel #:nodoc:
  Deprecation.deprecation_message_stream = STDERR
  #Deprecation.print_tracebacks = true
  class << self
    # call-seq:
    #   Sequel::Database.connect(conn_string)
    #   Sequel.connect(conn_string)
    #   Sequel.open(conn_string)
    #
    # Creates a new database object based on the supplied connection string.
    # The specified scheme determines the database class used, and the rest
    # of the string specifies the connection options. For example:
    #   DB = Sequel.open 'sqlite:///blog.db'
    def connect(*args)
      Database.connect(*args)
    end
    alias_method :open, :connect
    
    def single_threaded=(value)
      Database.single_threaded = value
    end

    def self.def_adapter_method(*adapters)
      adapters.each do |adapter|
        define_method(adapter) do |*args|
          raise(::Sequel::Error, "Wrong number of arguments, 0-2 arguments valid") if args.length > 2
          opts = {:adapter=>adapter.to_sym}
          opts[:database] = args.shift if args.length >= 1 && !(args[0].is_a?(Hash))
          opts.merge!(args[0]) if args[0].is_a?(Hash)
          ::Sequel::Database.connect(opts)
        end
      end
    end

    def_adapter_method(*Database::ADAPTERS)
  end
end
