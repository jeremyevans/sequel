require 'uri'

require File.join(File.dirname(__FILE__), 'schema')

module Sequel
  # A Database object represents a virtual connection to a database.
  # The Database class is meant to be subclassed by database adapters in order
  # to provide the functionality needed for executing queries.
  class Database
    # Constructs a new instance of a database connection with the specified
    # options hash.
    def initialize(opts = {})
      @opts = opts
    end

    # Returns a new dataset with the from method invoked.
    def from(*args); query.from(*args); end
    
    # Returns a new dataset with the select method invoked.
    def select(*args); query.select(*args); end

    # returns a new dataset with the from parameter set. For example,
    #   db[:posts].each {|p| puts p[:title]}
    def [](table)
      query.from(table)
    end

    # Returns a literal SQL representation of a value. This method is usually
    # overriden in database adapters.
    def literal(v)
      case v
      when String: "'%s'" % v
      else v.to_s
      end
    end
    
    # Creates a table. The easiest way to use this method is to provide a
    # block:
    #   DB.create_table :posts do
    #     primary_key :id, :serial
    #     column :title, :text
    #     column :content, :text
    #     index :title
    #   end
    def create_table(name, columns = nil, indexes = nil, &block)
      if block
        schema = Schema.new
        schema.create_table(name, &block)
        schema.create(self)
      else
        execute Schema.create_table_sql(name, columns, indexes)
      end
    end
    
    # Drops a table.
    def drop_table(name)
      execute Schema.drop_table_sql(name)
    end
    
    # Performs a brute-force check for the existance of a table. This method is
    # usually overriden in descendants.
    def table_exists?(name)
      from(name).first && true
    rescue
      false
    end
    
    @@adapters = Hash.new
    
    # Sets the adapter scheme for the database class. Call this method in
    # descendnants of Database to allow connection using a URL. For example:
    #   class DB2::Database < Sequel::Database
    #     set_adapter_scheme :db2
    #     ...
    #   end
    def self.set_adapter_scheme(scheme)
      @@adapters[scheme.to_sym] = self
    end
    
    # Converts a uri to an options hash. These options are then passed
    # to a newly created database object.
    def self.uri_to_options(uri)
      {
        :user => uri.user,
        :password => uri.password,
        :host => uri.host,
        :port => uri.port,
        :database => (uri.path =~ /\/(.*)/) && ($1)
      }
    end
    
    # call-seq:
    #   Sequel::Database.connect(conn_string)
    #   Sequel.connect(conn_string)
    #
    # Creates a new database object based on the supplied connection string.
    # The specified scheme determines the database class used, and the rest
    # of the string specifies the connection options. For example:
    #   DB = Sequel.connect('sqlite:///blog.db')
    def self.connect(conn_string)
      uri = URI.parse(conn_string)
      c = @@adapters[uri.scheme.to_sym]
      raise "Invalid database scheme" unless c
      c.new(c.uri_to_options(uri))
    end
  end
end

