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
          # If there is no escape_string instance method, but there is an
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

    PG_TYPES[17] = Class.new do
      def bytea(s) ::Sequel::SQL::Blob.new(Adapter.unescape_bytea(s)) end
    end.new.method(:bytea)

    # PGconn subclass for connection specific methods used with the
    # pg, postgres, or postgres-pr driver.
    class Adapter < ::PGconn
      DISCONNECT_ERROR_RE = /\Acould not receive data from server/
      
      self.translate_results = false if respond_to?(:translate_results=)
      
      # Hash of prepared statements for this connection.  Keys are
      # string names of the server side prepared statement, and values
      # are SQL strings.
      attr_reader(:prepared_statements) if SEQUEL_POSTGRES_USES_PG
      
      # Raise a Sequel::DatabaseDisconnectError if a PGError is raised and
      # the connection status cannot be determined or it is not OK.
      def check_disconnect_errors
        begin
          yield
        rescue PGError => e
          disconnect = false
          begin
            s = status
          rescue PGError
            disconnect = true
          end
          status_ok = (s == Adapter::CONNECTION_OK)
          disconnect ||= !status_ok
          disconnect ||= e.message =~ DISCONNECT_ERROR_RE
          disconnect ? raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError)) : raise
        ensure
          block if status_ok && !disconnect
        end
      end

      # Execute the given SQL with this connection.  If a block is given,
      # yield the results, otherwise, return the number of changed rows.
      def execute(sql, args=nil)
        args = args.map{|v| @db.bound_variable_arg(v, self)} if args
        q = check_disconnect_errors{execute_query(sql, args)}
        begin
          block_given? ? yield(q) : q.cmd_tuples
        ensure
          q.clear if q && q.respond_to?(:clear)
        end
      end

      private

      # Return the PGResult object that is returned by executing the given
      # sql and args.
      def execute_query(sql, args)
        @db.log_yield(sql, args){args ? async_exec(sql, args) : async_exec(sql)}
      end
    end
    
    # Database class for PostgreSQL databases used with Sequel and the
    # pg, postgres, or postgres-pr driver.
    class Database < Sequel::Database
      include Sequel::Postgres::DatabaseMethods

      INFINITE_TIMESTAMP_STRINGS = ['infinity'.freeze, '-infinity'.freeze].freeze
      INFINITE_DATETIME_VALUES = ([PLUS_INFINITY, MINUS_INFINITY] + INFINITE_TIMESTAMP_STRINGS).freeze
      
      set_adapter_scheme :postgres

      # Whether infinite timestamps should be converted on retrieval.  By default, no
      # conversion is done, so an error is raised if you attempt to retrieve an infinite
      # timestamp.  You can set this to :nil to convert to nil, :string to leave
      # as a string, or :float to convert to an infinite float.
      attr_accessor :convert_infinite_timestamps
      
      # Add the primary_keys and primary_key_sequences instance variables,
      # so we can get the correct return values for inserted rows.
      def initialize(*args)
        super
        @convert_infinite_timestamps = false
        initialize_postgres_adapter
      end

      # Convert given argument so that it can be used directly by pg.  Currently, pg doesn't
      # handle fractional seconds in Time/DateTime or blobs with "\0", and it won't ever
      # handle Sequel::SQLTime values correctly.  Only public for use by the adapter, shouldn't
      # be used by external code.
      def bound_variable_arg(arg, conn)
        case arg
        when Sequel::SQL::Blob
          conn.escape_bytea(arg)
        when Sequel::SQLTime
          literal(arg)
        when DateTime, Time
          literal(arg)
        else
          arg
        end
      end

      # Connects to the database.  In addition to the standard database
      # options, using the :encoding or :charset option changes the
      # client encoding for the connection, :connect_timeout is a
      # connection timeout in seconds, and :sslmode sets whether postgres's
      # sslmode.  :connect_timeout and :ssl_mode are only supported if the pg
      # driver is used.
      def connect(server)
        opts = server_opts(server)
        conn = if SEQUEL_POSTGRES_USES_PG
          connection_params = {
            :host => opts[:host],
            :port => opts[:port] || 5432,
            :dbname => opts[:database],
            :user => opts[:user],
            :password => opts[:password],
            :connect_timeout => opts[:connect_timeout] || 20,
            :sslmode => opts[:sslmode]
          }.delete_if { |key, value| blank_object?(value) }
          Adapter.connect(connection_params)
        else
          Adapter.connect(
            (opts[:host] unless blank_object?(opts[:host])),
            opts[:port] || 5432,
            nil, '',
            opts[:database],
            opts[:user],
            opts[:password]
          )
        end
        if encoding = opts[:encoding] || opts[:charset]
          if conn.respond_to?(:set_client_encoding)
            conn.set_client_encoding(encoding)
          else
            conn.async_exec("set client_encoding to '#{encoding}'")
          end
        end
        conn.instance_variable_set(:@db, self)
        conn.instance_variable_set(:@prepared_statements, {}) if SEQUEL_POSTGRES_USES_PG
        connection_configuration_sqls.each{|sql| conn.execute(sql)}
        conn
      end
      
      # Execute the given SQL with the given args on an available connection.
      def execute(sql, opts={}, &block)
        synchronize(opts[:server]){|conn| check_database_errors{_execute(conn, sql, opts, &block)}}
      end

      if SEQUEL_POSTGRES_USES_PG
        # +copy_table+ uses PostgreSQL's +COPY TO STDOUT+ SQL statement to return formatted
        # results directly to the caller.  This method is only supported if pg is the
        # underlying ruby driver.  This method should only be called if you want
        # results returned to the client.  If you are using +COPY TO+
        # with a filename, you should just use +run+ instead of this method.
        #
        # The table argument supports the following types:
        #
        # String :: Uses the first argument directly as literal SQL. If you are using
        #           a version of PostgreSQL before 9.0, you will probably want to
        #           use a string if you are using any options at all, as the syntax
        #           Sequel uses for options is only compatible with PostgreSQL 9.0+.
        # Dataset :: Uses a query instead of a table name when copying.
        # other :: Uses a table name (usually a symbol) when copying.
        # 
        # The following options are respected:
        #
        # :format :: The format to use.  text is the default, so this should be :csv or :binary.
        # :options :: An options SQL string to use, which should contain comma separated options.
        # :server :: The server on which to run the query.
        #
        # If a block is provided, the method continually yields to the block, one yield
        # per row.  If a block is not provided, a single string is returned with all
        # of the data.
        def copy_table(table, opts={})
          sql = if table.is_a?(String)
            sql = table
          else
            if opts[:options] || opts[:format]
              options = " ("
              options << "FORMAT #{opts[:format]}" if opts[:format]
              options << "#{', ' if opts[:format]}#{opts[:options]}" if opts[:options]
              options << ')'
            end
            table = if table.is_a?(::Sequel::Dataset)
              "(#{table.sql})"
            else
              literal(table)
            end
           sql = "COPY #{table} TO STDOUT#{options}"
          end
          synchronize(opts[:server]) do |conn| 
            conn.execute(sql)
            begin
              if block_given?
                while buf = conn.get_copy_data
                  yield buf
                end
                nil
              else
                b = ''
                b << buf while buf = conn.get_copy_data
                b
              end
            ensure
              raise DatabaseDisconnectError, "disconnecting as a partial COPY may leave the connection in an unusable state" if buf
            end
          end 
        end

        # +copy_into+ uses PostgreSQL's +COPY FROM STDIN+ SQL statement to do very fast inserts 
        # into a table using input preformatting in either CSV or PostgreSQL text format.
        # This method is only supported if pg is the underlying ruby driver.  This method should only be called if you want
        # results returned to the client.  If you are using +COPY FROM+
        # with a filename, you should just use +run+ instead of this method.
        #
        # The following options are respected:
        #
        # :columns :: The columns to insert into, with the same order as the columns in the
        #             input data.  If this isn't given, uses all columns in the table.
        # :data :: The data to copy to PostgreSQL, which should already be in CSV or PostgreSQL
        #          text format.  This can be either a string, or any object that responds to
        #          each and yields string.
        # :format :: The format to use.  text is the default, so this should be :csv or :binary.
        # :options :: An options SQL string to use, which should contain comma separated options.
        # :server :: The server on which to run the query.
        #
        # If a block is provided and :data option is not, this will yield to the block repeatedly.
        # The block should return a string, or nil to signal that it is finished.
        def copy_into(table, opts={})
          sql = "COPY #{literal(table)}"
          if cols = opts[:columns]
            sql << literal(Array(cols))
          end
          sql << " FROM STDIN"
          if opts[:options] || opts[:format]
            sql << " ("
            sql << "FORMAT #{opts[:format]}" if opts[:format]
            sql << "#{', ' if opts[:format]}#{opts[:options]}" if opts[:options]
            sql << ')'
          end
          
          data = opts[:data]
          data = Array(data) if data.is_a?(String)

          if block_given? && data
            raise Error, "Cannot provide both a :data option and a block to copy_into"
          elsif !block_given? && !data
            raise Error, "Must provide either a :data option or a block to copy_into"
          end

          synchronize(opts[:server]) do |conn| 
            conn.execute(sql)
            begin
              if block_given?
                while buf = yield
                  conn.put_copy_data(buf)
                end
              else
                data.each{|buf| conn.put_copy_data(buf)}
              end
            rescue Exception => e
              conn.put_copy_end("ruby exception occurred while copying data into PostgreSQL")
              raise
            ensure
              conn.put_copy_end unless e
              conn.get_result
            end
          end 
        end

        # Listens on the given channel (or multiple channels if channel is an array), waiting for notifications.
        # After a notification is received, or the timeout has passed, stops listening to the channel. Options:
        #
        # :after_listen :: An object that responds to +call+ that is called with the underlying connection after the LISTEN
        #                  statement is sent, but before the connection starts waiting for notifications.
        # :loop :: Whether to continually wait for notifications, instead of just waiting for a single
        #          notification. If this option is given, a block must be provided.  If this object responds to call, it is
        #          called with the underlying connection after each notification is received (after the block is called).
        #          If a :timeout option is used, and a callable object is given, the object will also be called if the
        #          timeout expires.  If :loop is used and you want to stop listening, you can either break from inside the
        #          block given to #listen, or you can throw :stop from inside the :loop object's call method or the block.
        # :server :: The server on which to listen, if the sharding support is being used.
        # :timeout :: How long to wait for a notification, in seconds (can provide a float value for
        #             fractional seconds).  If not given or nil, waits indefinitely.
        #
        # This method is only supported if pg is used as the underlying ruby driver.  It returns the
        # channel the notification was sent to (as a string), unless :loop was used, in which case it returns nil.
        # If a block is given, it is yielded 3 arguments:
        # * the channel the notification was sent to (as a string)
        # * the backend pid of the notifier (as an integer),
        # * and the payload of the notification (as a string or nil).
        def listen(channels, opts={}, &block)
          check_database_errors do
            synchronize(opts[:server]) do |conn|
              begin
                channels = Array(channels)
                channels.each{|channel| conn.execute("LISTEN #{dataset.send(:table_ref, channel)}")}
                opts[:after_listen].call(conn) if opts[:after_listen]
                timeout = opts[:timeout] ? [opts[:timeout]] : []
                if l = opts[:loop]
                  raise Error, 'calling #listen with :loop requires a block' unless block
                  loop_call = l.respond_to?(:call)
                  catch(:stop) do
                    loop do
                      conn.wait_for_notify(*timeout, &block)
                      l.call(conn) if loop_call
                    end
                  end
                  nil
                else
                  conn.wait_for_notify(*timeout, &block)
                end
              ensure
                conn.execute("UNLISTEN *")
              end
            end
          end
        end
      end

      # If convert_infinite_timestamps is true and the value is infinite, return an appropriate
      # value based on the convert_infinite_timestamps setting.
      def to_application_timestamp(value)
        if c = convert_infinite_timestamps
          case value
          when *INFINITE_TIMESTAMP_STRINGS
            infinite_timestamp_value(value)
          else
            super
          end
        else
          super
        end
      end
        
      private

      # Execute the given SQL string or prepared statement on the connection object.
      def _execute(conn, sql, opts, &block)
        if sql.is_a?(Symbol)
          execute_prepared_statement(conn, sql, opts, &block)
        else
          conn.execute(sql, opts[:arguments], &block)
        end
      end

      # Execute the prepared statement name with the given arguments on the connection.
      def _execute_prepared_statement(conn, ps_name, args, opts)
        conn.exec_prepared(ps_name, args)
      end

      # Convert exceptions raised from the block into DatabaseErrors.
      def check_database_errors
        begin
          yield
        rescue => e
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        end
      end

      # Set the DateStyle to ISO if configured, for faster date parsing.
      def connection_configuration_sqls
        sqls = super
        sqls << "SET DateStyle = 'ISO'" if Postgres.use_iso_date_format
        sqls
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
      # of rows changed.
      def execute_prepared_statement(conn, name, opts={}, &block)
        ps = prepared_statement(name)
        sql = ps.prepared_sql
        ps_name = name.to_s

        if args = opts[:arguments]
          args = args.map{|arg| bound_variable_arg(arg, conn)}
        end

        unless conn.prepared_statements[ps_name] == sql
          conn.execute("DEALLOCATE #{ps_name}") if conn.prepared_statements.include?(ps_name)
          conn.check_disconnect_errors{log_yield("PREPARE #{ps_name} AS #{sql}"){conn.prepare(ps_name, sql)}}
          conn.prepared_statements[ps_name] = sql
        end

        log_sql = "EXECUTE #{ps_name}"
        if ps.log_sql
          log_sql << " ("
          log_sql << sql
          log_sql << ")"
        end

        q = conn.check_disconnect_errors{log_yield(log_sql, args){_execute_prepared_statement(conn, ps_name, args, opts)}}
        begin
          block_given? ? yield(q) : q.cmd_tuples
        ensure
          q.clear if q && q.respond_to?(:clear)
        end
      end

      # Return an appropriate value for the given infinite timestamp string.
      def infinite_timestamp_value(value)
        case convert_infinite_timestamps
        when :nil
          nil
        when :string
          value
        else
          value == 'infinity' ? PLUS_INFINITY : MINUS_INFINITY
        end
      end
      
      # Don't log, since logging is done by the underlying connection.
      def log_connection_execute(conn, sql)
        conn.execute(sql)
      end

      # If the value is an infinite value (either an infinite float or a string returned by
      # by PostgreSQL for an infinite timestamp), return it without converting it if
      # convert_infinite_timestamps is set.
      def typecast_value_datetime(value)
        if convert_infinite_timestamps
          case value
          when *INFINITE_DATETIME_VALUES
            value
          else
            super
          end
        else
          super
        end
      end
    end
    
    # Dataset class for PostgreSQL datasets that use the pg, postgres, or
    # postgres-pr driver.
    class Dataset < Sequel::Dataset
      include Sequel::Postgres::DatasetMethods

      Database::DatasetClass = self
      APOS = Sequel::Dataset::APOS
      
      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql)
        return cursor_fetch_rows(sql){|h| yield h} if @opts[:cursor]
        execute(sql){|res| yield_hash_rows(res, fetch_rows_set_cols(res)){|h| yield h}}
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

          # Always assume a prepared argument.
          def prepared_arg?(k)
           true
          end
        end

        # Allow use of bind arguments for PostgreSQL using the pg driver.
        module BindArgumentMethods
          include ArgumentMapper
          include ::Sequel::Postgres::DatasetMethods::PreparedStatementMethods
          
          private
          
          # Execute the given SQL with the stored bind arguments.
          def execute(sql, opts={}, &block)
            super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
          end
          
          # Same as execute, explicit due to intricacies of alias and super.
          def execute_dui(sql, opts={}, &block)
            super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
          end
        end
        
        # Allow use of server side prepared statements for PostgreSQL using the
        # pg driver.
        module PreparedStatementMethods
          include BindArgumentMethods

          # Raise a more obvious error if you attempt to call a unnamed prepared statement.
          def call(*)
            raise Error, "Cannot call prepared statement without a name" if prepared_statement_name.nil?
            super
          end
          
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
            db.set_prepared_statement(name, ps)
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
      def cursor_fetch_rows(sql)
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
              yield_hash_rows(res, cols){|h| yield h}
              return if res.ntuples < rows_per_fetch
            end
            loop do
              execute(fetch_sql) do |res|
                yield_hash_rows(res, cols){|h| yield h}
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
        procs = db.conversion_procs
        res.nfields.times do |fieldnum|
          cols << [fieldnum, procs[res.ftype(fieldnum)], output_identifier(res.fname(fieldnum))]
        end
        @columns = cols.map{|c| c.at(2)}
        cols
      end
      
      # Use the driver's escape_bytea
      def literal_blob_append(sql, v)
        sql << APOS << db.synchronize{|c| c.escape_bytea(v)} << APOS
      end
      
      # Use the driver's escape_string
      def literal_string_append(sql, v)
        sql << APOS << db.synchronize{|c| c.escape_string(v)} << APOS
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

if SEQUEL_POSTGRES_USES_PG
  begin
    require 'sequel_pg'
  rescue LoadError
    if RUBY_PLATFORM =~ /mingw|mswin/
      begin
        require "#{RUBY_VERSION[0...3]}/sequel_pg"
      rescue LoadError
      end
    end
  end
end
