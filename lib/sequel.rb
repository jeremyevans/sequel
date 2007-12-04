require 'metaid'
require 'bigdecimal'
require 'bigdecimal/util'

files = %w[
  core_ext core_sql array_keys error connection_pool pretty_table
  dataset migration model schema database worker
]
dir = File.join(File.dirname(__FILE__), 'sequel')
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
        case args.size
        when 1: # Sequel.dbi(db_name)
          opts = args[0].is_a?(Hash) ? args[0] : {:database => args[0]}
        when 0 # Sequel.dbi
          opts = {}
        else # Sequel.dbi(db_name, opts)
          opts = args[1].merge(:database => args[0])
        end
      rescue
        raise SequelError, "Invalid parameters specified"
      end
      c.new(opts)
    end
  end
end

class Object
  def Sequel(*args)
    Sequel.connect(*args)
  end
end
