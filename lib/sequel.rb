dir = File.join(File.dirname(__FILE__), 'sequel')
require File.join(dir, 'core_ext')
require File.join(dir, 'error')
require File.join(dir, 'database')
require File.join(dir, 'connection_pool')
require File.join(dir, 'schema')
require File.join(dir, 'pretty_table')
require File.join(dir, 'expressions')
require File.join(dir, 'dataset')
require File.join(dir, 'model')

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
  end
end

class Object
  def Sequel(uri)
    Sequel.connect(uri)
  end
end