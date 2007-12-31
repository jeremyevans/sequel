require "metaid"
require "bigdecimal"
require "bigdecimal/util"

files = %w[
  core_ext core_sql array_keys exceptions connection_pool pretty_table
  dataset migration model schema database worker
]
dir = File.join(File.dirname(__FILE__), "sequel")
files.each {|f| require(File.join(dir, f))}

module Sequel #:nodoc:
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
    
    def method_missing(m, *args)
      c = Database.adapter_class(m)
      begin
        # three ways to invoke this:
        # 0 arguments: Sequel.dbi 
        # 1 argument:  Sequel.dbi(db_name)
        # more args:   Sequel.dbi(db_name, opts)
        case args.size
        when 0
          opts = {}
        when 1
          opts = args[0].is_a?(Hash) ? args[0] : {:database => args[0]}
        else
          opts = args[1].merge(:database => args[0])
        end
      rescue
        raise Error::AdapterNotFound, "Unknown adapter (#{m})"
      end
      c.new(opts)
    end
    
    # stub for Sequel::Model()
    def Model(*args)
      require 'sequel_model'
      if respond_to?(:Model)
        send(:Model, *args)
      else
        raise LoadError
      end
    rescue LoadError
      raise SequelError, "The sequel_model library could not be found. In order to use Sequel models please install sequel_model."
    end
  end
end

class Object
  def Sequel(*args)
    Sequel.connect(*args)
  end
end
