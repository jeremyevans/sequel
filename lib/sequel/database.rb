require 'uri'

module Sequel
  # A Database object represents a virtual connection to a database.
  # The Database class is meant to be subclassed by database adapters in order
  # to provide the functionality needed for executing queries.
  class Database
    attr_reader :opts, :pool, :logger
    
    # Constructs a new instance of a database connection with the specified
    # options hash.
    #
    # Sequel::Database is an abstract class that is not useful by itself.
    def initialize(opts = {}, &block)
      Model.database_opened(self)
      @opts = opts
      
      # Determine if the DB is single threaded or multi threaded
      @single_threaded = opts[:single_threaded] || @@single_threaded
      # Construct pool
      if @single_threaded
        @pool = SingleThreadedPool.new(&block)
      else
        @pool = ConnectionPool.new(opts[:max_connections] || 4, &block)
      end
      @pool.connection_proc = block || proc {connect}

      @logger = opts[:logger]
    end
    
    def connect
      raise NotImplementedError, "#connect should be overriden by adapters"
    end
    
    def multi_threaded?
      !@single_threaded
    end
    
    def single_threaded?
      @single_threaded
    end
    
    def uri
      uri = URI::Generic.new(
        self.class.adapter_scheme.to_s,
        nil,
        @opts[:host],
        @opts[:port],
        nil,
        "/#{@opts[:database]}",
        nil,
        nil,
        nil
      )
      uri.user = @opts[:user]
      uri.password = @opts[:password]
      uri.to_s
    end
    alias url uri # Because I don't care much for the semantic difference.
    
    # Returns a blank dataset
    def dataset
      ds = Sequel::Dataset.new(self)
    end
    
    def fetch(sql, *args, &block)
      ds = dataset
      sql = sql.gsub('?') {|m|  ds.literal(args.shift)}
      if block
        ds.fetch_rows(sql, &block)
      else
        Enumerable::Enumerator.new(ds, :fetch_rows, sql)
      end
    end
    
    # Converts a query block into a dataset. For more information see 
    # Dataset#query.
    def query(&block)
      dataset.query(&block)
    end
    
    # Returns a new dataset with the from method invoked. If a block is given,
    # it is used as a filter on the dataset.
    def from(*args, &block)
      ds = dataset.from(*args)
      block ? ds.filter(&block) : ds
    end
    
    # Returns a new dataset with the select method invoked.
    def select(*args); dataset.select(*args); end
    
    alias_method :[], :from

    def execute(sql)
      raise NotImplementedError
    end
    
    # Executes the supplied SQL statement. The SQL can be supplied as a string
    # or as an array of strings. If an array is give, comments and excessive 
    # white space are removed. See also Array#to_sql.
    def <<(sql); execute((Array === sql) ? sql.to_sql : sql); end
    
    # Acquires a database connection, yielding it to the passed block.
    def synchronize(&block)
      @pool.hold(&block)
    end

    # Returns true if there is a database connection
    def test_connection
      @pool.hold {|conn|}
      true
    end
    
    include Dataset::SQL
    include Schema::SQL
    
    # default serial primary key definition. this should be overriden for each adapter.
    def serial_primary_key_options
      {:primary_key => true, :type => :integer, :auto_increment => true}
    end
    
    # Creates a table. The easiest way to use this method is to provide a
    # block:
    #   DB.create_table :posts do
    #     primary_key :id, :serial
    #     column :title, :text
    #     column :content, :text
    #     index :title
    #   end
    def create_table(name, &block)
      g = Schema::Generator.new(self, name, &block)
      create_table_sql_list(*g.create_info).each {|sta| execute(sta)}
    end
    
    # Drops a table.
    def drop_table(*names)
      execute(names.map {|n| drop_table_sql(n)}.join)
    end
    
    # Performs a brute-force check for the existance of a table. This method is
    # usually overriden in descendants.
    def table_exists?(name)
      if respond_to?(:tables)
        tables.include?(name.to_sym)
      else
        from(name).first && true
      end
    rescue
      false
    end
    
    SQL_BEGIN = 'BEGIN'.freeze
    SQL_COMMIT = 'COMMIT'.freeze
    SQL_ROLLBACK = 'ROLLBACK'.freeze

    # A simple implementation of SQL transactions. Nested transactions are not 
    # supported - calling #transaction within a transaction will reuse the 
    # current transaction. May be overridden for databases that support nested 
    # transactions.
    def transaction
      @pool.hold do |conn|
        @transactions ||= []
        if @transactions.include? Thread.current
          return yield(conn)
        end
        conn.execute(SQL_BEGIN)
        begin
          @transactions << Thread.current
          result = yield(conn)
          conn.execute(SQL_COMMIT)
          result
        rescue => e
          conn.execute(SQL_ROLLBACK)
          raise e unless SequelRollbackError === e
        ensure
          @transactions.delete(Thread.current)
        end
      end
    end

    @@adapters = Hash.new
    
    # Sets the adapter scheme for the Database class. Call this method in
    # descendnants of Database to allow connection using a URL. For example the
    # following:
    #   class DB2::Database < Sequel::Database
    #     set_adapter_scheme :db2
    #     ...
    #   end
    # would allow connection using:
    #   Sequel.open('db2://user:password@dbserver/mydb')
    def self.set_adapter_scheme(scheme)
      @scheme = scheme
      @@adapters[scheme.to_sym] = self
      
      # Define convenience method for this database class
      db_class = self
      Sequel.meta_def(scheme) do |*args|
        begin
          case args.size
          when 1: # Sequel.dbi(db_name)
            opts = {:database => args[0]}
          when 0 # Sequel.dbi
            opts = {}
          else # Sequel.dbi(db_name, opts)
            opts = args[1].merge(:database => args[0])
          end
        rescue
          raise SequelError, "Invalid parameters specified"
        end
        db_class.new(opts)
      end
    end
    
    # Returns the scheme for the Database class.
    def self.adapter_scheme
      @scheme
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
    #   Sequel.open(conn_string)
    #
    # Creates a new database object based on the supplied connection string.
    # The specified scheme determines the database class used, and the rest
    # of the string specifies the connection options. For example:
    #   DB = Sequel.open 'sqlite:///blog.db'
    def self.connect(conn_string, more_opts = nil)
      uri = URI.parse(conn_string)
      c = @@adapters[uri.scheme.to_sym]
      raise SequelError, "Invalid database scheme" unless c
      c.new(c.uri_to_options(uri).merge(more_opts || {}))
    end
    
    @@single_threaded = false
    
    def self.single_threaded=(value)
      @@single_threaded = value
    end
  end
end

