require 'metaid'

files = %w[
  core_ext error database connection_pool
  schema pretty_table expressions dataset migration model
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
  end
end

class Object
  def Sequel(uri)
    Sequel.connect(uri)
  end
end