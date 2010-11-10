require 'sqlite3'
begin
  SQLite3::Database.instance_method(:type_translation)
rescue
  raise(Sequel::Error, "SQLite3::Database#type_translation is not defined.  If you are using the sqlite3 gem, please install the sqlite3-ruby gem.")
end
Sequel.require 'adapters/shared/sqlite'

module Sequel
  # Top level module for holding all SQLite-related modules and classes
  # for Sequel.
  module SQLite
    UNIX_EPOCH_TIME_FORMAT = /\A\d+\z/.freeze
    SQLITE_TYPES = {}
    SQLITE_TYPE_PROCS = {
      %w'timestamp datetime' => proc{|v| Sequel.database_to_application_timestamp(v)},
      %w'date' => proc{|v| Sequel.string_to_date(v)},
      %w'boolean' => proc{|v| v == 't'},
      %w'numeric decimal money' => proc{|v| BigDecimal.new(v) rescue v},
      ['float', 'real', 'double precision'] => proc{|v| Float(v) rescue v},
      %w'blob' => proc{|v| ::Sequel::SQL::Blob.new(v)}
    }
    SQLITE_TYPE_PROCS.each do |k,v|
      k.each{|n| SQLITE_TYPES[n] = v}
    end
    
    # Database class for SQLite databases used with Sequel and the
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
        
        connection_pragmas.each{|s| log_yield(s){db.execute_batch(s)}}
        
        class << db
          attr_reader :prepared_statements
        end
        db.instance_variable_set(:@prepared_statements, {})
        
        db
      end
      
      # Return instance of Sequel::SQLite::Dataset with the given options.
      def dataset(opts = nil)
        SQLite::Dataset.new(self, opts)
      end
      
      # Run the given SQL with the given arguments and yield each row.
      def execute(sql, opts={}, &block)
        _execute(:select, sql, opts, &block)
      end

      # Run the given SQL with the given arguments and return the number of changed rows.
      def execute_dui(sql, opts={})
        _execute(:update, sql, opts)
      end
      
      # Drop any prepared statements on the connection when executing DDL.  This is because
      # prepared statements lock the table in such a way that you can't drop or alter the
      # table while a prepared statement that references it still exists.
      def execute_ddl(sql, opts={})
        synchronize(opts[:server]) do |conn|
          conn.prepared_statements.values.each{|cps, s| cps.close}
          conn.prepared_statements.clear
          super
        end
      end
      
      # Run the given SQL with the given arguments and return the last inserted row id.
      def execute_insert(sql, opts={})
        _execute(:insert, sql, opts)
      end
      
      # Run the given SQL with the given arguments and return the first value of the first row.
      def single_value(sql, opts={})
        _execute(:single_value, sql, opts)
      end
      
      private
      
      # Yield an available connection.  Rescue
      # any SQLite3::Exceptions and turn them into DatabaseErrors.
      def _execute(type, sql, opts, &block)
        begin
          synchronize(opts[:server]) do |conn|
            return execute_prepared_statement(conn, type, sql, opts, &block) if sql.is_a?(Symbol)
            log_args = opts[:arguments]
            args = opts.fetch(:arguments, [])
            case type
            when :select
              log_yield(sql, log_args){conn.query(sql, args, &block)}
            when :single_value
              log_yield(sql, log_args){conn.get_first_value(sql, args)}
            when :insert
              log_yield(sql, log_args){conn.execute(sql, args)}
              conn.last_insert_row_id
            when :update
              log_yield(sql, log_args){conn.execute_batch(sql, args)}
              conn.changes
            end
          end
        rescue SQLite3::Exception => e
          raise_error(e)
        end
      end
      
      # The SQLite adapter does not need the pool to convert exceptions.
      # Also, force the max connections to 1 if a memory database is being
      # used, as otherwise each connection gets a separate database.
      def connection_pool_default_options
        o = super.dup
        # Default to only a single connection if a memory database is used,
        # because otherwise each connection will get a separate database
        o[:max_connections] = 1 if @opts[:database] == ':memory:' || blank_object?(@opts[:database])
        o
      end
      
      # Execute a prepared statement on the database using the given name.
      def execute_prepared_statement(conn, type, name, opts, &block)
        ps = prepared_statements[name]
        sql = ps.prepared_sql
        args = opts[:arguments]
        if cpsa = conn.prepared_statements[name]
          cps, cps_sql = cpsa
          if cps_sql != sql
            cps.close
            cps = nil
          end
        end
        unless cps
          cps = conn.prepare(sql)
          conn.prepared_statements[name] = [cps, sql]
        end
        if block
          log_yield("Executing prepared statement #{name}", args){cps.execute(args, &block)}
        else
          log_yield("Executing prepared statement #{name}", args){cps.execute!(args){|r|}}
          case type
          when :insert
            conn.last_insert_row_id
          when :update
            conn.changes
          end
        end
      end
      
      # The main error class that SQLite3 raises
      def database_error_classes
        [SQLite3::Exception]
      end

      # Disconnect given connections from the database.
      def disconnect_connection(c)
        c.close
      end
    end
    
    # Dataset class for SQLite datasets that use the ruby-sqlite3 driver.
    class Dataset < Sequel::Dataset
      include ::Sequel::SQLite::DatasetMethods
      
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
      module BindArgumentMethods
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

      module PreparedStatementMethods
        include BindArgumentMethods
          
        private
          
        # Execute the stored prepared statement name and the stored bind
        # arguments instead of the SQL given.
        def execute(sql, opts={}, &block)
          super(prepared_statement_name, opts, &block)
        end
         
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
          super(prepared_statement_name, opts, &block)
        end
          
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_insert(sql, opts={}, &block)
          super(prepared_statement_name, opts, &block)
        end
      end
        
      # Execute the given type of statement with the hash of values.
      def call(type, bind_vars={}, *values, &block)
        ps = to_prepared_statement(type, values)
        ps.extend(BindArgumentMethods)
        ps.call(bind_vars, &block)
      end
      
      # Yield a hash for each row in the dataset.
      def fetch_rows(sql)
        execute(sql) do |result|
          i = -1
          type_procs = result.types.map{|t| SQLITE_TYPES[t]}
          cols = result.columns.map{|c| i+=1; [output_identifier(c), i, type_procs[i]]}
          @columns = cols.map{|c| c.first}
          result.each do |values|
            row = {}
            cols.each do |name,i,type_proc|
              v = values[i]
              if type_proc && v.is_a?(String)
                v = type_proc.call(v)
              end
              row[name] = v
            end
            yield row
          end
        end
      end
      
      # Prepare the given type of query with the given name and store
      # it in the database.  Note that a new native prepared statement is
      # created on each call to this prepared statement.
      def prepare(type, name=nil, *values)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        if name
          ps.prepared_statement_name = name
          db.prepared_statements[name] = ps
        end
        ps.prepared_sql
        ps
      end
      
      private
      
      # Quote the string using the adapter class method.
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
