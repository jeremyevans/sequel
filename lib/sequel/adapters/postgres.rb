# frozen-string-literal: true

Sequel.require 'adapters/shared/postgres'

begin 
  require 'pg' 

  # Work around postgres-pr 0.7.0+ which ships with a pg.rb file
  raise LoadError unless defined?(PGconn::CONNECTION_OK)

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
            if Sequel::Postgres.force_standard_strings
              str.gsub("'", "''")
            else
              raise Sequel::Error, "string escaping not supported with this postgres driver.  Try using ruby-pg, ruby-postgres, or postgres-pr."
            end
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
      # The underlying exception classes to reraise as disconnect errors
      # instead of regular database errors.
      DISCONNECT_ERROR_CLASSES = [IOError, Errno::EPIPE, Errno::ECONNRESET]
      if defined?(::PG::ConnectionBad)
        DISCONNECT_ERROR_CLASSES << ::PG::ConnectionBad
      end
      
      disconnect_errors = [
        'could not receive data from server',
        'no connection to the server',
        'connection not open',
        'terminating connection due to administrator command',
        'PQconsumeInput() '
       ]

      # Since exception class based disconnect checking may not work,
      # also trying parsing the exception message to look for disconnect
      # errors.
      DISCONNECT_ERROR_RE = /\A#{Regexp.union(disconnect_errors)}/
      
      self.translate_results = false if respond_to?(:translate_results=)
      
      # Hash of prepared statements for this connection.  Keys are
      # string names of the server side prepared statement, and values
      # are SQL strings.
      attr_reader(:prepared_statements) if SEQUEL_POSTGRES_USES_PG
      
      # Raise a Sequel::DatabaseDisconnectError if a one of the disconnect
      # error classes is raised, or a PGError is raised and the connection
      # status cannot be determined or it is not OK.
      def check_disconnect_errors
        begin
          yield
        rescue *DISCONNECT_ERROR_CLASSES => e
          disconnect = true
          raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError))
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
      
      set_adapter_scheme :postgresql
      set_adapter_scheme :postgres

      # Whether infinite timestamps/dates should be converted on retrieval.  By default, no
      # conversion is done, so an error is raised if you attempt to retrieve an infinite
      # timestamp/date.  You can set this to :nil to convert to nil, :string to leave
      # as a string, or :float to convert to an infinite float.
      attr_reader :convert_infinite_timestamps

      # Convert given argument so that it can be used directly by pg.  Currently, pg doesn't
      # handle fractional seconds in Time/DateTime or blobs with "\0", and it won't ever
      # handle Sequel::SQLTime values correctly.  Only public for use by the adapter, shouldn't
      # be used by external code.
      def bound_variable_arg(arg, conn)
        case arg
        when Sequel::SQL::Blob
          {:value=>arg, :type=>17, :format=>1}
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
      # connection timeout in seconds, :sslmode sets whether postgres's
      # sslmode, and :notice_receiver handles server notices in a proc.
      # :connect_timeout, :ssl_mode, and :notice_receiver are only supported
      # if the pg driver is used.
      def connect(server)
        opts = server_opts(server)
        if SEQUEL_POSTGRES_USES_PG
          connection_params = {
            :host => opts[:host],
            :port => opts[:port] || 5432,
            :dbname => opts[:database],
            :user => opts[:user],
            :password => opts[:password],
            :connect_timeout => opts[:connect_timeout] || 20,
            :sslmode => opts[:sslmode]
          }.delete_if { |key, value| blank_object?(value) }
          conn = Adapter.connect(connection_params)

          conn.instance_variable_set(:@prepared_statements, {})

          if receiver = opts[:notice_receiver]
            conn.set_notice_receiver(&receiver)
          end
        else
          conn = Adapter.connect(
            (opts[:host] unless blank_object?(opts[:host])),
            opts[:port] || 5432,
            nil, '',
            opts[:database],
            opts[:user],
            opts[:password]
          )
        end

        conn.instance_variable_set(:@db, self)

        if encoding = opts[:encoding] || opts[:charset]
          if conn.respond_to?(:set_client_encoding)
            conn.set_client_encoding(encoding)
          else
            conn.async_exec("set client_encoding to '#{encoding}'")
          end
        end

        connection_configuration_sqls.each{|sql| conn.execute(sql)}
        conn
      end
      
      # Set whether to allow infinite timestamps/dates.  Make sure the
      # conversion proc for date reflects that setting.
      def convert_infinite_timestamps=(v)
        @convert_infinite_timestamps = case v
        when Symbol
          v
        when 'nil'
          :nil
        when 'string'
          :string
        when 'float'
          :float
        when String
          typecast_value_boolean(v)
        else
          false
        end

        pr = old_pr = @use_iso_date_format ? TYPE_TRANSLATOR.method(:date) : Sequel.method(:string_to_date)
        if v
          pr = lambda do |val|
            case val
            when *INFINITE_TIMESTAMP_STRINGS
              infinite_timestamp_value(val)
            else
              old_pr.call(val)
            end
          end
        end
        conversion_procs[1082] = pr
      end

      # Disconnect given connection
      def disconnect_connection(conn)
        begin
          conn.finish
        rescue PGError, IOError
        end
      end

      if SEQUEL_POSTGRES_USES_PG && Object.const_defined?(:PG) && ::PG.const_defined?(:Constants) && ::PG::Constants.const_defined?(:PG_DIAG_SCHEMA_NAME)
        # Return a hash of information about the related PGError (or Sequel::DatabaseError that
        # wraps a PGError), with the following entries:
        #
        # :schema :: The schema name related to the error
        # :table :: The table name related to the error
        # :column :: the column name related to the error
        # :constraint :: The constraint name related to the error
        # :type :: The datatype name related to the error
        #
        # This requires a PostgreSQL 9.3+ server and 9.3+ client library,
        # and ruby-pg 0.16.0+ to be supported.
        def error_info(e)
          e = e.wrapped_exception if e.is_a?(DatabaseError)
          r = e.result
          h = {}
          h[:schema] = r.error_field(::PG::PG_DIAG_SCHEMA_NAME)
          h[:table] = r.error_field(::PG::PG_DIAG_TABLE_NAME)
          h[:column] = r.error_field(::PG::PG_DIAG_COLUMN_NAME)
          h[:constraint] = r.error_field(::PG::PG_DIAG_CONSTRAINT_NAME)
          h[:type] = r.error_field(::PG::PG_DIAG_DATATYPE_NAME)
          h
        end
      end
      
      # Execute the given SQL with the given args on an available connection.
      def execute(sql, opts=OPTS, &block)
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
        def copy_table(table, opts=OPTS)
          synchronize(opts[:server]) do |conn|
            conn.execute(copy_table_sql(table, opts))
            begin
              if block_given?
                while buf = conn.get_copy_data
                  yield buf
                end
                nil
              else
                b = String.new
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
        # This method is only supported if pg 0.14.0+ is the underlying ruby driver.
        # This method should only be called if you want
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
        def copy_into(table, opts=OPTS)
          data = opts[:data]
          data = Array(data) if data.is_a?(String)

          if block_given? && data
            raise Error, "Cannot provide both a :data option and a block to copy_into"
          elsif !block_given? && !data
            raise Error, "Must provide either a :data option or a block to copy_into"
          end

          synchronize(opts[:server]) do |conn|
            conn.execute(copy_into_sql(table, opts))
            begin
              if block_given?
                while buf = yield
                  conn.put_copy_data(buf)
                end
              else
                data.each{|buff| conn.put_copy_data(buff)}
              end
            rescue Exception => e
              conn.put_copy_end("ruby exception occurred while copying data into PostgreSQL")
            ensure
              conn.put_copy_end unless e
              while res = conn.get_result
                raise e if e
                check_database_errors{res.check}
              end
            end
          end 
        end

        # Listens on the given channel (or multiple channels if channel is an array), waiting for notifications.
        # After a notification is received, or the timeout has passed, stops listening to the channel. Options:
        #
        # :after_listen :: An object that responds to +call+ that is called with the underlying connection after the LISTEN
        #                  statement is sent, but before the connection starts waiting for notifications.
        # :loop :: Whether to continually wait for notifications, instead of just waiting for a single
        #          notification. If this option is given, a block must be provided.  If this object responds to +call+, it is
        #          called with the underlying connection after each notification is received (after the block is called).
        #          If a :timeout option is used, and a callable object is given, the object will also be called if the
        #          timeout expires.  If :loop is used and you want to stop listening, you can either break from inside the
        #          block given to #listen, or you can throw :stop from inside the :loop object's call method or the block.
        # :server :: The server on which to listen, if the sharding support is being used.
        # :timeout :: How long to wait for a notification, in seconds (can provide a float value for fractional seconds).
        #             If this object responds to +call+, it will be called and should return the number of seconds to wait.
        #             If the loop option is also specified, the object will be called on each iteration to obtain a new
        #             timeout value.  If not given or nil, waits indefinitely.
        #
        # This method is only supported if pg is used as the underlying ruby driver.  It returns the
        # channel the notification was sent to (as a string), unless :loop was used, in which case it returns nil.
        # If a block is given, it is yielded 3 arguments:
        # * the channel the notification was sent to (as a string)
        # * the backend pid of the notifier (as an integer),
        # * and the payload of the notification (as a string or nil).
        def listen(channels, opts=OPTS, &block)
          check_database_errors do
            synchronize(opts[:server]) do |conn|
              begin
                channels = Array(channels)
                channels.each do |channel|
                  sql = "LISTEN ".dup
                  dataset.send(:identifier_append, sql, channel)
                  conn.execute(sql)
                end
                opts[:after_listen].call(conn) if opts[:after_listen]
                timeout = opts[:timeout]
                if timeout
                  timeout_block = timeout.respond_to?(:call) ? timeout : proc{timeout}
                end

                if l = opts[:loop]
                  raise Error, 'calling #listen with :loop requires a block' unless block
                  loop_call = l.respond_to?(:call)
                  catch(:stop) do
                    loop do
                      t = timeout_block ? [timeout_block.call] : []
                      conn.wait_for_notify(*t, &block)
                      l.call(conn) if loop_call
                    end
                  end
                  nil
                else
                  t = timeout_block ? [timeout_block.call] : []
                  conn.wait_for_notify(*t, &block)
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
        if convert_infinite_timestamps
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

      # Add the primary_keys and primary_key_sequences instance variables,
      # so we can get the correct return values for inserted rows.
      def adapter_initialize
        @use_iso_date_format = typecast_value_boolean(@opts.fetch(:use_iso_date_format, Postgres.use_iso_date_format))
        initialize_postgres_adapter
        conversion_procs[1082] = TYPE_TRANSLATOR.method(:date) if @use_iso_date_format
        self.convert_infinite_timestamps = @opts[:convert_infinite_timestamps]
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
        sqls << "SET DateStyle = 'ISO'" if @use_iso_date_format
        sqls
      end

      def database_error_classes
        [PGError]
      end

      def database_exception_sqlstate(exception, opts)
        if exception.respond_to?(:result) && (result = exception.result)
          result.error_field(::PGresult::PG_DIAG_SQLSTATE)
        end
      end

      # Execute the prepared statement with the given name on an available
      # connection, using the given args.  If the connection has not prepared
      # a statement with the given name yet, prepare it.  If the connection
      # has prepared a statement with the same name and different SQL,
      # deallocate that statement first and then prepare this statement.
      # If a block is given, yield the result, otherwise, return the number
      # of rows changed.
      def execute_prepared_statement(conn, name, opts=OPTS, &block)
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
          log_sql += " ("
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
      def typecast_value_date(value)
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
      DEFAULT_CURSOR_NAME = 'sequel_cursor'.freeze
      
      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql)
        return cursor_fetch_rows(sql){|h| yield h} if @opts[:cursor]
        execute(sql){|res| yield_hash_rows(res, fetch_rows_set_cols(res)){|h| yield h}}
      end
      
      # Use a cursor for paging.
      def paged_each(opts=OPTS, &block)
        use_cursor(opts).each(&block)
      end

      # Uses a cursor for fetching records, instead of fetching the entire result
      # set at once.  Note this uses a transaction around the cursor usage by
      # default and can be changed using `hold: true` as described below.
      # Cursors can be used to process large datasets without holding all rows
      # in memory (which is what the underlying drivers may do by default).
      # Options:
      #
      # :cursor_name :: The name assigned to the cursor (default 'sequel_cursor').
      #                 Nested cursors require different names.
      # :hold :: Declare the cursor WITH HOLD and don't use transaction around the
      #          cursor usage.
      # :rows_per_fetch :: The number of rows per fetch (default 1000).  Higher
      #                    numbers result in fewer queries but greater memory use.
      #
      # Usage:
      #
      #   DB[:huge_table].use_cursor.each{|row| p row}
      #   DB[:huge_table].use_cursor(:rows_per_fetch=>10000).each{|row| p row}
      #   DB[:huge_table].use_cursor(:cursor_name=>'my_cursor').each{|row| p row}      
      #
      # This is untested with the prepared statement/bound variable support,
      # and unlikely to work with either.
      def use_cursor(opts=OPTS)
        clone(:cursor=>{:rows_per_fetch=>1000}.merge!(opts))
      end

      # Replace the WHERE clause with one that uses CURRENT OF with the given
      # cursor name (or the default cursor name).  This allows you to update a
      # large dataset by updating individual rows while processing the dataset
      # via a cursor:
      #
      #   DB[:huge_table].use_cursor(:rows_per_fetch=>1).each do |row|
      #     DB[:huge_table].where_current_of.update(:column=>ruby_method(row))
      #   end
      def where_current_of(cursor_name=DEFAULT_CURSOR_NAME)
        clone(:where=>Sequel.lit(['CURRENT OF '], Sequel.identifier(cursor_name)))
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
          
          def prepared_arg(k)
            y = k
            if i = prepared_args.index(y)
              i += 1
            else
              prepared_args << y
              i = prepared_args.length
            end
            LiteralString.new("#{prepared_arg_placeholder}#{i}")
          end

          # Always assume a prepared argument.
          def prepared_arg?(k)
           true
          end
        end

        BindArgumentMethods = prepared_statements_module(:bind, [ArgumentMapper, ::Sequel::Postgres::DatasetMethods::PreparedStatementMethods], %w'execute execute_dui')

        PreparedStatementMethods = prepared_statements_module(:prepare, BindArgumentMethods, %w'execute execute_dui') do
          # Raise a more obvious error if you attempt to call a unnamed prepared statement.
          def call(*)
            raise Error, "Cannot call prepared statement without a name" if prepared_statement_name.nil?
            super
          end
        end
        
        # Execute the given type of statement with the hash of values.
        def call(type, bind_vars=OPTS, *values, &block)
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
        cursor = @opts[:cursor]
        hold = cursor[:hold]
        cursor_name = quote_identifier(cursor[:cursor_name] || DEFAULT_CURSOR_NAME)
        rows_per_fetch = cursor[:rows_per_fetch].to_i

        db.send(*(hold ? [:synchronize, server_opts[:server]] : [:transaction, server_opts])) do 
          begin
            execute_ddl("DECLARE #{cursor_name} NO SCROLL CURSOR WITH#{'OUT' unless hold} HOLD FOR #{sql}", server_opts)
            rows_per_fetch = 1000 if rows_per_fetch <= 0
            fetch_sql = "FETCH FORWARD #{rows_per_fetch} FROM #{cursor_name}"
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
          rescue Exception => e
            raise
          ensure
            begin
              execute_ddl("CLOSE #{cursor_name}", server_opts)
            rescue
              raise e if e
              raise
            end
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
        sql << APOS << db.synchronize(@opts[:server]){|c| c.escape_bytea(v)} << APOS
      end
      
      # Use the driver's escape_string
      def literal_string_append(sql, v)
        sql << APOS << db.synchronize(@opts[:server]){|c| c.escape_string(v)} << APOS
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

if SEQUEL_POSTGRES_USES_PG && !ENV['NO_SEQUEL_PG']
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
