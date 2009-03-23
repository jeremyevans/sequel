require 'sqlite3'
Sequel.require 'adapters/shared/sqlite'

module Sequel
  # Top level module for holding all SQLite-related modules and classes
  # for Sequel.
  module SQLite
    # Database class for PostgreSQL databases used with Sequel and the
    # ruby-sqlite3 driver.
    class Database < Sequel::Database
      UNIX_EPOCH_TIME_FORMAT = /\A\d+\z/.freeze
      include ::Sequel::SQLite::DatabaseMethods
      
      set_adapter_scheme :sqlite
      
      # Mimic the file:// uri, by having 2 preceding slashes specify a relative
      # path, and 3 preceding slashes specify an absolute path.
      def self.uri_to_options(uri) # :nodoc:
        { :database => (uri.host.nil? && uri.path == '/') ? nil : "#{uri.host}#{uri.path}" }
      end
      
      private_class_method :uri_to_options
      
      # Connect to the database.  Since SQLite is a file based database,
      # the only options available are :database (to specify the database
      # name), and :timeout, to specify how long to wait for the database to
      # be available if it is locked, given in milliseconds (default is 5000).
      def connect(server)
        opts = server_opts(server)
        opts[:database] = ':memory:' if blank_object?(opts[:database])
        db = ::SQLite3::Database.new(opts[:database])
        db.busy_timeout(opts.fetch(:timeout, 5000))
        db.type_translation = true
        
        # Handle datetime's with Sequel.datetime_class
        prok = proc do |t,v|
          v = Time.at(v.to_i).iso8601 if UNIX_EPOCH_TIME_FORMAT.match(v)
          Sequel.string_to_datetime(v)
        end
        db.translator.add_translator("timestamp", &prok)
        db.translator.add_translator("datetime", &prok)
        
        # Handle numeric values with BigDecimal
        prok = proc{|t,v| BigDecimal.new(v) rescue v}
        db.translator.add_translator("numeric", &prok)
        db.translator.add_translator("decimal", &prok)
        db.translator.add_translator("money", &prok)
        
        # Handle floating point values with Float
        prok = proc{|t,v| Float(v) rescue v}
        db.translator.add_translator("float", &prok)
        db.translator.add_translator("real", &prok)
        db.translator.add_translator("double precision", &prok)
        
        # Handle blob values with Sequel::SQL::Blob
        db.translator.add_translator("blob"){|t,v| ::Sequel::SQL::Blob.new(v)}
        
        db
      end
      
      # Return instance of Sequel::SQLite::Dataset with the given options.
      def dataset(opts = nil)
        SQLite::Dataset.new(self, opts)
      end
      
      # Run the given SQL with the given arguments and return the number of changed rows.
      def execute_dui(sql, opts={})
        _execute(sql, opts){|conn| conn.execute_batch(sql, opts[:arguments]); conn.changes}
      end
      
      # Run the given SQL with the given arguments and return the last inserted row id.
      def execute_insert(sql, opts={})
        _execute(sql, opts){|conn| conn.execute(sql, opts[:arguments]); conn.last_insert_row_id}
      end
      
      # Run the given SQL with the given arguments and yield each row.
      def execute(sql, opts={}, &block)
        _execute(sql, opts){|conn| conn.query(sql, opts[:arguments], &block)}
      end
      
      # Run the given SQL with the given arguments and return the first value of the first row.
      def single_value(sql, opts={})
        _execute(sql, opts){|conn| conn.get_first_value(sql, opts[:arguments])}
      end
      
      # Use the native driver transaction method if there isn't already a transaction
      # in progress on the connection, always yielding a connection inside a transaction
      # transaction.
      def transaction(opts={})
        unless opts.is_a?(Hash)
          Deprecation.deprecate('Passing an argument other than a Hash to Database#transaction', "Use DB.transaction(:server=>#{opts.inspect})") 
          opts = {:server=>opts}
        end
        synchronize(opts[:server]) do |conn|
          return yield(conn) if conn.transaction_active?
          begin
            result = nil
            log_info('Transaction.begin')
            conn.transaction{result = yield(conn)}
            result
          rescue ::Exception => e
            log_info('Transaction.rollback')
            transaction_error(e, SQLite3::Exception)
          ensure
            log_info('Transaction.commit') unless e
          end
        end
      end
      
      private
      
      # Log the SQL and the arguments, and yield an available connection.  Rescue
      # any SQLite3::Exceptions and turn the into DatabaseErrors.
      def _execute(sql, opts)
        begin
          log_info(sql, opts[:arguments])
          synchronize(opts[:server]){|conn| yield conn}
        rescue SQLite3::Exception => e
          raise_error(e)
        end
      end
      
      # SQLite does not need the pool to convert exceptions.
      # Also, force the max connections to 1 if a memory database is being
      # used, as otherwise each connection gets a separate database.
      def connection_pool_default_options
        o = super.merge(:pool_convert_exceptions=>false)
        # Default to only a single connection if a memory database is used,
        # because otherwise each connection will get a separate database
        o[:max_connections] = 1 if @opts[:database] == ':memory:' || blank_object?(@opts[:database])
        o
      end

      # Disconnect given connections from the database.
      def disconnect_connection(c)
        c.close
      end
    end
    
    # Dataset class for SQLite datasets that use the ruby-sqlite3 driver.
    class Dataset < Sequel::Dataset
      include ::Sequel::SQLite::DatasetMethods
      
      EXPLAIN = 'EXPLAIN %s'.freeze
      PREPARED_ARG_PLACEHOLDER = ':'.freeze
      
      # SQLite already supports named bind arguments, so use directly.
      module ArgumentMapper
        include Sequel::Dataset::ArgumentMapper
        
        protected
        
        # Return a hash with the same values as the given hash,
        # but with the keys converted to strings.
        def map_to_prepared_args(hash)
          args = {}
          hash.each{|k,v| args[k.to_s] = v}
          args
        end
        
        private
        
        # SQLite uses a : before the name of the argument for named
        # arguments.
        def prepared_arg(k)
          LiteralString.new("#{prepared_arg_placeholder}#{k}")
        end
      end
      
      # SQLite prepared statement uses a new prepared statement each time
      # it is called, but it does use the bind arguments.
      module PreparedStatementMethods
        include ArgumentMapper
        
        private
        
        # Run execute_select on the database with the given SQL and the stored
        # bind arguments.
        def execute(sql, opts={}, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_insert(sql, opts={}, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end
      end
      
      # Prepare an unnamed statement of the given type and call it with the
      # given values.
      def call(type, hash, values=nil, &block)
        prepare(type, nil, values).call(hash, &block)
      end
      
      # Return an array of strings specifying a query explanation for the
      # current dataset.
      def explain
        res = []
        @db.result_set(EXPLAIN % select_sql(opts), nil) {|r| res << r}
        res
      end
      
      # Yield a hash for each row in the dataset.
      def fetch_rows(sql)
        execute(sql) do |result|
          @columns = result.columns.map{|c| output_identifier(c)}
          column_count = @columns.size
          result.each do |values|
            row = {}
            column_count.times {|i| row[@columns[i]] = values[i]}
            yield row
          end
        end
      end
      
      # Prepare the given type of query with the given name and store
      # it in the database.  Note that a new native prepared statement is
      # created on each call to this prepared statement.
      def prepare(type, name=nil, values=nil)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        db.prepared_statements[name] = ps if name
        ps
      end
      
      private
      
      def literal_string(v)
        "'#{::SQLite3::Database.quote(v)}'"
      end

      # SQLite uses a : before the name of the argument as a placeholder.
      def prepared_arg_placeholder
        PREPARED_ARG_PLACEHOLDER
      end
    end
  end
end
