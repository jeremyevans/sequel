Sequel.require 'adapters/shared/postgres'

begin 
  require 'pg' 
  SEQUEL_POSTGRES_USES_PG = true
rescue LoadError => e 
  SEQUEL_POSTGRES_USES_PG = false
  begin
    require 'postgres'
    # Attempt to get uniform behavior for the PGconn object no matter
    # if pg, postgres, or postgres-pr is used.
    class PGconn
      unless method_defined?(:escape_string)
        if self.respond_to?(:escape)
          # If there is no escape_string instead method, but there is an
          # escape class method, use that instead.
          def escape_string(str)
            Sequel::Postgres.force_standard_strings ? str.gsub("'", "''") : self.class.escape(str)
          end
        else
          # Raise an error if no valid string escaping method can be found.
          def escape_string(obj)
            raise Sequel::Error, "string escaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
          end
        end
      end
      unless method_defined?(:escape_bytea)
        if self.respond_to?(:escape_bytea)
          # If there is no escape_bytea instance method, but there is an
          # escape_bytea class method, use that instead.
          def escape_bytea(obj)
            self.class.escape_bytea(obj)
          end
        else
          begin
            require 'postgres-pr/typeconv/conv'
            require 'postgres-pr/typeconv/bytea'
            extend Postgres::Conversion
            # If we are using postgres-pr, use the encode_bytea method from
            # that.
            def escape_bytea(obj)
              self.class.encode_bytea(obj)
            end
            instance_eval{alias unescape_bytea decode_bytea}
          rescue
            # If no valid bytea escaping method can be found, create one that
            # raises an error
            def escape_bytea(obj)
              raise Sequel::Error, "bytea escaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
            end
            # If no valid bytea unescaping method can be found, create one that
            # raises an error
            def self.unescape_bytea(obj)
              raise Sequel::Error, "bytea unescaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
            end
          end
        end
      end
      alias_method :finish, :close unless method_defined?(:finish)
      alias_method :async_exec, :exec unless method_defined?(:async_exec)
      unless method_defined?(:block)
        def block(timeout=nil)
        end
      end
      unless defined?(CONNECTION_OK)
        CONNECTION_OK = -1
      end
      unless method_defined?(:status)
        def status
          CONNECTION_OK
        end
      end
    end
    class PGresult 
      alias_method :nfields, :num_fields unless method_defined?(:nfields) 
      alias_method :ntuples, :num_tuples unless method_defined?(:ntuples) 
      alias_method :ftype, :type unless method_defined?(:ftype) 
      alias_method :fname, :fieldname unless method_defined?(:fname) 
      alias_method :cmd_tuples, :cmdtuples unless method_defined?(:cmd_tuples) 
    end 
  rescue LoadError 
    raise e 
  end 
end

module Sequel
  Dataset::NON_SQL_OPTIONS << :cursor
  module Postgres
    CONVERTED_EXCEPTIONS << PGError
    
    # Hash with integer keys and proc values for converting PostgreSQL types.
    PG_TYPES = {}

    # Use a single proc for each type to conserve memory
    PG_TYPE_PROCS  = {
      [16] =>  lambda{|s| s == 't'}, # boolean
      [17] => lambda{|s| ::Sequel::SQL::Blob.new(Adapter.unescape_bytea(s))}, # bytea
      [20, 21, 22, 23, 26] => lambda{|s| s.to_i}, # integer
      [700, 701] => lambda{|s| s.to_f}, # float
      [790, 1700] => lambda{|s| BigDecimal.new(s)}, # numeric
      [1082] => lambda{|s| @use_iso_date_format ? Date.new(*s.split("-").map{|x| x.to_i}) : Sequel.string_to_date(s)}, # date
      [1083, 1266] => lambda{|s| Sequel.string_to_time(s)}, # time
      [1114, 1184] => lambda{|s| Sequel.database_to_application_timestamp(s)}, # timestamp
    }
    PG_TYPE_PROCS.each do |k,v|
      k.each{|n| PG_TYPES[n] = v}
    end
    
    @use_iso_date_format = true

    class << self
      # As an optimization, Sequel sets the date style to ISO, so that PostgreSQL provides
      # the date in a known format that Sequel can parse faster.  This can be turned off
      # if you require a date style other than ISO.
      attr_accessor :use_iso_date_format
    end
    
    # PGconn subclass for connection specific methods used with the
    # pg, postgres, or postgres-pr driver.
    class Adapter < ::PGconn
      include Sequel::Postgres::AdapterMethods
      self.translate_results = false if respond_to?(:translate_results=)
      
      # Hash of prepared statements for this connection.  Keys are
      # string names of the server side prepared statement, and values
      # are SQL strings.
      attr_reader(:prepared_statements) if SEQUEL_POSTGRES_USES_PG
      
      # Apply connection settings for this connection.  Current sets
      # the date style to ISO in order make Date object creation in ruby faster,
      # if Postgres.use_iso_date_format is true.
      def apply_connection_settings
        super
        if Postgres.use_iso_date_format
          sql = "SET DateStyle = 'ISO'"
          @db.log_info(sql)
          execute(sql)
        end
        @prepared_statements = {} if SEQUEL_POSTGRES_USES_PG
      end
      
      # Raise a Sequel::DatabaseDisconnectError if a PGError is raised and
      # the connection status cannot be determined or it is not OK.
      def check_disconnect_errors
        begin
          yield
        rescue PGError =>e
          begin
            s = status
          rescue PGError
            raise Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError)
          end
          status_ok = (s == Adapter::CONNECTION_OK)
          status_ok ? raise : raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError))
        ensure
          block if status_ok
        end
      end

      # Execute the given SQL with this connection.  If a block is given,
      # yield the results, otherwise, return the number of changed rows.
      def execute(sql, args=nil)
        q = check_disconnect_errors{args ? async_exec(sql, args) : async_exec(sql)}
        begin
          block_given? ? yield(q) : q.cmd_tuples
        ensure
          q.clear
        end
      end

      private
      
      # Return the requested values for the given row.
      def single_value(r)
        r.getvalue(0, 0) unless r.nil? || (r.ntuples == 0)
      end
    end
    
    # Database class for PostgreSQL databases used with Sequel and the
    # pg, postgres, or postgres-pr driver.
    class Database < Sequel::Database
      include Sequel::Postgres::DatabaseMethods
      
      set_adapter_scheme :postgres
      
      # Add the primary_keys and primary_key_sequences instance variables,
      # so we can get the correct return values for inserted rows.
      def initialize(*args)
        super
        @primary_keys = {}
        @primary_key_sequences = {}
      end

      # Connects to the database.  In addition to the standard database
      # options, using the :encoding or :charset option changes the
      # client encoding for the connection.
      def connect(server)
        opts = server_opts(server)
        conn = Adapter.connect(
          (opts[:host] unless blank_object?(opts[:host])),
          opts[:port] || 5432,
          nil, '',
          opts[:database],
          opts[:user],
          opts[:password]
        )
        if encoding = opts[:encoding] || opts[:charset]
          if conn.respond_to?(:set_client_encoding)
            conn.set_client_encoding(encoding)
          else
            conn.async_exec("set client_encoding to '#{encoding}'")
          end
        end
        conn.db = self
        conn.apply_connection_settings
        conn
      end
      
      # Return instance of Sequel::Postgres::Dataset with the given options.
      def dataset(opts = nil)
        Postgres::Dataset.new(self, opts)
      end
      
      # Execute the given SQL with the given args on an available connection.
      def execute(sql, opts={}, &block)
        check_database_errors do
          return execute_prepared_statement(sql, opts, &block) if Symbol === sql
          log_info(sql, opts[:arguments])
          synchronize(opts[:server]){|conn| conn.execute(sql, opts[:arguments], &block)}
        end
      end
      
      # Insert the values into the table and return the primary key (if
      # automatically generated).
      def execute_insert(sql, opts={})
        return execute(sql, opts) if Symbol === sql
        check_database_errors do
          log_info(sql, opts[:arguments])
          synchronize(opts[:server]) do |conn|
            conn.execute(sql, opts[:arguments])
            insert_result(conn, opts[:table], opts[:values])
          end
        end
      end
      
      private
      
      # Convert exceptions raised from the block into DatabaseErrors.
      def check_database_errors
        begin
          yield
        rescue => e
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        end
      end

      # Disconnect given connection
      def disconnect_connection(conn)
        begin
          conn.finish
        rescue PGError
        end
      end
      
      # Execute the prepared statement with the given name on an available
      # connection, using the given args.  If the connection has not prepared
      # a statement with the given name yet, prepare it.  If the connection
      # has prepared a statement with the same name and different SQL,
      # deallocate that statement first and then prepare this statement.
      # If a block is given, yield the result, otherwise, return the number
      # of rows changed.  If the :insert option is passed, return the value
      # of the primary key for the last inserted row.
      def execute_prepared_statement(name, opts={})
        ps = prepared_statements[name]
        sql = ps.prepared_sql
        ps_name = name.to_s
        args = opts[:arguments]
        synchronize(opts[:server]) do |conn|
          unless conn.prepared_statements[ps_name] == sql
            if conn.prepared_statements.include?(ps_name)
              s = "DEALLOCATE #{ps_name}"
              log_info(s)
              conn.execute(s) unless conn.prepared_statements[ps_name] == sql
            end
            conn.prepared_statements[ps_name] = sql
            log_info("PREPARE #{ps_name} AS #{sql}")
            conn.check_disconnect_errors{conn.prepare(ps_name, sql)}
          end
          log_info("EXECUTE #{ps_name}", args)
          q = conn.check_disconnect_errors{conn.exec_prepared(ps_name, args)}
          if opts[:table] && opts[:values]
            insert_result(conn, opts[:table], opts[:values])
          else
            begin
              block_given? ? yield(q) : q.cmd_tuples
            ensure
              q.clear
            end
          end
        end
      end
    end
    
    # Dataset class for PostgreSQL datasets that use the pg, postgres, or
    # postgres-pr driver.
    class Dataset < Sequel::Dataset
      include Sequel::Postgres::DatasetMethods
      
      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql, &block)
        return cursor_fetch_rows(sql, &block) if @opts[:cursor]
        execute(sql){|res| yield_hash_rows(res, fetch_rows_set_cols(res), &block)}
      end
      
      # Uses a cursor for fetching records, instead of fetching the entire result
      # set at once.  Can be used to process large datasets without holding
      # all rows in memory (which is what the underlying drivers do
      # by default). Options:
      #
      # * :rows_per_fetch - the number of rows per fetch (default 1000).  Higher
      #   numbers result in fewer queries but greater memory use.
      #
      # Usage:
      #
      #   DB[:huge_table].use_cursor.each{|row| p row}
      #   DB[:huge_table].use_cursor(:rows_per_fetch=>10000).each{|row| p row}
      #
      # This is untested with the prepared statement/bound variable support,
      # and unlikely to work with either.
      def use_cursor(opts={})
        clone(:cursor=>{:rows_per_fetch=>1000}.merge(opts))
      end

      if SEQUEL_POSTGRES_USES_PG
        
        PREPARED_ARG_PLACEHOLDER = LiteralString.new('$').freeze
        
        # PostgreSQL specific argument mapper used for mapping the named
        # argument hash to a array with numbered arguments.  Only used with
        # the pg driver.
        module ArgumentMapper
          include Sequel::Dataset::ArgumentMapper
          
          protected
          
          # An array of bound variable values for this query, in the correct order.
          def map_to_prepared_args(hash)
            prepared_args.map{|k| hash[k.to_sym]}
          end

          private
          
          # PostgreSQL most of the time requires type information for each of
          # arguments to a prepared statement.  Handle this by allowing the
          # named argument to have a __* suffix, with the * being the type.
          # In the generated SQL, cast the bound argument to that type to
          # elminate ambiguity (and PostgreSQL from raising an exception).
          def prepared_arg(k)
            y, type = k.to_s.split("__")
            if i = prepared_args.index(y)
              i += 1
            else
              prepared_args << y
              i = prepared_args.length
            end
            LiteralString.new("#{prepared_arg_placeholder}#{i}#{"::#{type}" if type}")
          end
        end

        # Allow use of bind arguments for PostgreSQL using the pg driver.
        module BindArgumentMethods
          include ArgumentMapper
          
          private
          
          # Execute the given SQL with the stored bind arguments.
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
        
        # Allow use of server side prepared statements for PostgreSQL using the
        # pg driver.
        module PreparedStatementMethods
          include BindArgumentMethods
          include ::Sequel::Postgres::DatasetMethods::PreparedStatementMethods
          
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
        
        # Prepare the given type of statement with the given name, and store
        # it in the database to be called later.
        def prepare(type, name=nil, *values)
          ps = to_prepared_statement(type, values)
          ps.extend(PreparedStatementMethods)
          if name
            ps.prepared_statement_name = name
            db.prepared_statements[name] = ps
          end
          ps
        end
        
        private
        
        # PostgreSQL uses $N for placeholders instead of ?, so use a $
        # as the placeholder.
        def prepared_arg_placeholder
          PREPARED_ARG_PLACEHOLDER
        end
      end
      
      private
      
      # Use a cursor to fetch groups of records at a time, yielding them to the block.
      def cursor_fetch_rows(sql, &block)
        server_opts = {:server=>@opts[:server] || :read_only}
        db.transaction(server_opts) do 
          begin
            execute_ddl("DECLARE sequel_cursor NO SCROLL CURSOR WITHOUT HOLD FOR #{sql}", server_opts)
            rows_per_fetch = @opts[:cursor][:rows_per_fetch].to_i
            rows_per_fetch = 1000 if rows_per_fetch <= 0
            fetch_sql = "FETCH FORWARD #{rows_per_fetch} FROM sequel_cursor"
            cols = nil
            # Load columns only in the first fetch, so subsequent fetches are faster
            execute(fetch_sql) do |res|
              cols = fetch_rows_set_cols(res)
              yield_hash_rows(res, cols, &block)
              return if res.ntuples < rows_per_fetch
            end
            loop do
              execute(fetch_sql) do |res|
                yield_hash_rows(res, cols, &block)
                return if res.ntuples < rows_per_fetch
              end
            end
          ensure
            execute_ddl("CLOSE sequel_cursor", server_opts)
          end
        end
      end
      
      # Set the @columns based on the result set, and return the array of
      # field numers, type conversion procs, and name symbol arrays.
      def fetch_rows_set_cols(res)
        cols = []
        res.nfields.times do |fieldnum|
          cols << [fieldnum, PG_TYPES[res.ftype(fieldnum)], output_identifier(res.fname(fieldnum))]
        end
        @columns = cols.map{|c| c.at(2)}
        cols
      end
      
      # Use the driver's escape_bytea
      def literal_blob(v)
        db.synchronize{|c| "'#{c.escape_bytea(v)}'"}
      end
      
      # Use the driver's escape_string
      def literal_string(v)
        db.synchronize{|c| "'#{c.escape_string(v)}'"}
      end
      
      # For each row in the result set, yield a hash with column name symbol
      # keys and typecasted values.
      def yield_hash_rows(res, cols)
        res.ntuples.times do |recnum|
          converted_rec = {}
          cols.each do |fieldnum, type_proc, fieldsym|
            value = res.getvalue(recnum, fieldnum)
            converted_rec[fieldsym] = (value && type_proc) ? type_proc.call(value) : value
          end
          yield converted_rec
        end
      end
    end
  end
end
