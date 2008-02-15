require 'uri'

module Sequel
  # A Database object represents a virtual connection to a database.
  # The Database class is meant to be subclassed by database adapters in order
  # to provide the functionality needed for executing queries.
  class Database
    attr_reader :opts, :pool
    attr_accessor :logger
    
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
    
    # Connects to the database. This method should be overriden by descendants.
    def connect
      raise NotImplementedError, "#connect should be overriden by adapters"
    end
    
    # Disconnects from the database. This method should be overriden by 
    # descendants.
    def disconnect
      raise NotImplementedError, "#disconnect should be overriden by adapters"
    end
    
    # Returns true if the database is using a multi-threaded connection pool.
    def multi_threaded?
      !@single_threaded
    end
    
    # Returns true if the database is using a single-threaded connection pool.
    def single_threaded?
      @single_threaded
    end
    
    # Returns the URI identifying the database.
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
      uri.password = @opts[:password] if uri.user
      uri.to_s
    end
    alias url uri # Because I don't care much for the semantic difference.
    
    # Returns a blank dataset
    def dataset
      ds = Sequel::Dataset.new(self)
    end
    
    # Fetches records for an arbitrary SQL statement. If a block is given,
    # it is used to iterate over the records:
    #
    #   DB.fetch('SELECT * FROM items') {|r| p r}
    #
    # If a block is not given, the method returns a dataset instance:
    #
    #   DB.fetch('SELECT * FROM items').print
    #
    # Fetch can also perform parameterized queries for protection against SQL
    # injection:
    #
    #   DB.fetch('SELECT * FROM items WHERE name = ?', my_name).print
    #
    # A short-hand form for Database#fetch is Database#[]:
    #
    #   DB['SELECT * FROM items'].each {|r| p r}
    #
    def fetch(sql, *args, &block)
      ds = dataset
      sql = sql.gsub('?') {|m|  ds.literal(args.shift)}
      if block
        ds.fetch_rows(sql, &block)
      else
        ds.opts[:sql] = sql
        ds
      end
    end
    alias_method :>>, :fetch
    
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
    
    # Returns a dataset from the database. If the first argument is a string,
    # the method acts as an alias for Database#fetch, returning a dataset for
    # arbitrary SQL:
    #
    #   DB['SELECT * FROM items WHERE name = ?', my_name].print
    #
    # Otherwise, the dataset returned has its from option set to the given
    # arguments:
    #
    #   DB[:items].sql #=> "SELECT * FROM items"
    #
    def [](*args)
      (String === args.first) ? fetch(*args) : from(*args)
    end
    
    # Returns a single value from the database, e.g.:
    #
    #   # SELECT 1
    #   DB.get(1) #=> 1 
    #
    #   # SELECT version()
    #   DB.get(:version[]) #=> ...
    def get(expr)
      dataset.get(expr)
    end
    
    # Raises a Sequel::Error::NotImplemented. This method is overriden in descendants.
    def execute(sql)
      raise NotImplementedError, "#execute should be overriden by adapters"
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
    
    # include Dataset::SQL
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
      g = Schema::Generator.new(self, &block)
      create_table_sql_list(name, *g.create_info).each {|sql| execute(sql)}
    end
    
    # Forcibly creates a table. If the table already exists it is dropped.
    def create_table!(name, &block)
      drop_table(name) rescue nil
      create_table(name, &block)
    end
    
    # Drops one or more tables corresponding to the given table names.
    def drop_table(*names)
      names.each {|n| execute(drop_table_sql(n))}
    end
    
    # Renames a table:
    #
    #   DB.tables #=> [:items]
    #   DB.rename_table :items, :old_items
    #   DB.tables #=> [:old_items]
    def rename_table(*args)
      execute(rename_table_sql(*args))
    end
    
    # Alters the given table with the specified block. Here are the currently
    # available operations:
    #
    #   DB.alter_table :items do
    #     add_column :category, :text, :default => 'ruby'
    #     drop_column :category
    #     rename_column :cntr, :counter
    #     set_column_type :value, :float
    #     set_column_default :value, :float
    #     add_index [:group, :category]
    #     drop_index [:group, :category]
    #   end
    #
    # Note that #add_column accepts all the options available for column
    # definitions using create_table, and #add_index accepts all the options
    # available for index definition.
    def alter_table(name, &block)
      g = Schema::AlterTableGenerator.new(self, &block)
      alter_table_sql_list(name, g.operations).each {|sql| execute(sql)}
    end
    
    # Adds a column to the specified table. This method expects a column name,
    # a datatype and optionally a hash with additional constraints and options:
    #
    #   DB.add_column :items, :name, :text, :unique => true, :null => false
    #   DB.add_column :items, :category, :text, :default => 'ruby'
    def add_column(table, *args)
      alter_table(table) {add_column(*args)}
    end
    
    # Removes a column from the specified table:
    #
    #   DB.drop_column :items, :category
    def drop_column(table, *args)
      alter_table(table) {drop_column(*args)}
    end
    
    # Renames a column in the specified table. This method expects the current
    # column name and the new column name:
    #
    #   DB.rename_column :items, :cntr, :counter
    def rename_column(table, *args)
      alter_table(table) {rename_column(*args)}
    end
    
    # Set the data type for the given column in the given table:
    #
    #   DB.set_column_type :items, :price, :float
    def set_column_type(table, *args)
      alter_table(table) {set_column_type(*args)}
    end
    
    # Sets the default value for the given column in the given table:
    #
    #   DB.set_column_default :items, :category, 'perl!'
    def set_column_default(table, *args)
      alter_table(table) {set_column_default(*args)}
    end
    
    # Adds an index to a table for the given columns:
    # 
    #   DB.add_index :posts, :title
    #   DB.add_index :posts, [:author, :title], :unique => true
    def add_index(table, *args)
      alter_table(table) {add_index(*args)}
    end
    
    # Removes an index for the given table and column/s:
    #
    #   DB.drop_index :posts, :title
    #   DB.drop_index :posts, [:author, :title]
    def drop_index(table, *args)
      alter_table(table) {drop_index(*args)}
    end
    
    # Returns true if the given table exists.
    def table_exists?(name)
      if respond_to?(:tables)
        tables.include?(name.to_sym)
      else
        from(name).first
        true
      end
    rescue
      false
    end
    
    # Creates a view based on a dataset or an SQL string:
    #
    #   DB.create_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
    #   DB.create_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
    def create_view(name, source)
      source = source.sql if source.is_a?(Dataset)
      execute("CREATE VIEW #{name} AS #{source}")
    end
    
    # Creates a view, replacing it if it already exists:
    #
    #   DB.create_or_replace_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
    #   DB.create_or_replace_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
    def create_or_replace_view(name, source)
      source = source.sql if source.is_a?(Dataset)
      execute("CREATE OR REPLACE VIEW #{name} AS #{source}")
    end
    
    # Drops a view:
    #
    #   DB.drop_view(:cheap_items)
    def drop_view(name)
      execute("DROP VIEW #{name}")
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
          raise e unless Error::Rollback === e
        ensure
          @transactions.delete(Thread.current)
        end
      end
    end
    
    # Returns a string representation of the database object including the
    # class name and the connection URI.
    def inspect
      '#<%s: %s>' % [self.class.to_s, uri.inspect]
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
    
    def self.adapter_class(scheme)
      adapter_name = scheme.to_s =~ /\-/ ? scheme.to_s.gsub('-', '_').to_sym : scheme.to_sym
      scheme = scheme.to_sym
      
      if (klass = @@adapters[scheme]).nil?
        # attempt to load the adapter file
        begin
          require File.join(File.dirname(__FILE__), "adapters/#{scheme}")
        rescue LoadError => e
          raise Error::AdapterNotFound, "Could not load #{scheme} adapter:\n  #{e.message}"
        end
        
        # make sure we actually loaded the adapter
        if (klass = @@adapters[scheme]).nil?
          raise Error::AdapterNotFound, "Could not load #{scheme} adapter"
        end
      end
      return klass
    end
        
    # call-seq:
    #   Sequel::Database.connect(conn_string)
    #   Sequel::Database.connect(opts)
    #   Sequel.connect(conn_string)
    #   Sequel.connect(opts)
    #   Sequel.open(conn_string)
    #   Sequel.open(opts)
    #
    # Creates a new database object based on the supplied connection string
    # and or options. If a URI is used, the URI scheme determines the database
    # class used, and the rest of the string specifies the connection options. 
    # For example:
    #
    #   DB = Sequel.open 'sqlite:///blog.db'
    #
    # The second form of this method takes an options:
    #
    #   DB = Sequel.open :adapter => :sqlite, :database => 'blog.db'
    def self.connect(conn_string, opts = nil)
      if conn_string.is_a?(String)
        uri = URI.parse(conn_string)
        scheme = uri.scheme
        scheme = :dbi if scheme =~ /^dbi-(.+)/
        c = adapter_class(scheme)
        opts = c.uri_to_options(uri).merge(opts || {})
      else
        opts = conn_string.merge(opts || {})
        c = adapter_class(opts[:adapter] || opts['adapter'])
      end
      # process opts a bit
      opts = opts.inject({}) do |m, kv| k, v = *kv
        k = :user if k == 'username'
        m[k.to_sym] = v
        m
      end
      c.new(opts)
    end
    
    @@single_threaded = false
    
    # Sets the default single_threaded mode for new databases.
    def self.single_threaded=(value)
      @@single_threaded = value
    end
  end
end

