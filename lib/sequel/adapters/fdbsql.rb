require 'pg'

Sequel.require 'adapters/utils/pg_types'
Sequel.require 'adapters/shared/fdbsql'

module Sequel
  module Fdbsql
    CONVERTED_EXCEPTIONS << PGError

    # Database class for the FoundationDB SQL Layer used with Sequel and the
    # pg driver
    class Database < Sequel::Database
      include Sequel::Fdbsql::DatabaseMethods

      set_adapter_scheme :fdbsql

      # Connects to the database. In addition to the standard database options,
      # :connect_timeout is a connection timeout in seconds,
      # :sslmode sets whether to use ssl, and
      # :notice_receiver handles server notices in a proc.
      def connect(server)
        opts = server_opts(server)
        Connection.new(self, opts)
      end

      # Execute the given SQL with the given args on an available connection.
      def execute(sql, opts = {}, &block)
        res = nil
        synchronize(opts[:server]) do |conn|
          res = check_database_errors do
            if sql.is_a?(Symbol)
              execute_prepared_statement(conn, sql, opts, &block)
            else
              log_yield(sql) do
                conn.query(sql, opts[:arguments])
              end
            end
          end
          yield res if block_given?
          res.cmd_tuples
        end
      end

      def server_version
        return @server_version if @server_version

        version = get{VERSION{}}
        unless ver = version.match(/(\d+)\.(\d+)\.(\d+)/)
          raise Error, "No match when checking FDB SQL Layer version: #{version}"
        end

        @server_version = (100 * ver[1].to_i + ver[2].to_i) * 100 + ver[3].to_i
      end

      private

      def database_exception_sqlstate(exception, opts)
        if exception.respond_to?(:result) && (result = exception.result)
          result.error_field(::PGresult::PG_DIAG_SQLSTATE)
        end
      end

      def execute_prepared_statement(conn, name, opts=OPTS, &block)
        statement = prepared_statement(name)
        sql = statement.prepared_sql
        ps_name = name.to_s
        if args = opts[:arguments]
          args = args.map{|arg| bound_variable_arg(arg, conn)}
        end
        begin
          # create prepared statement if it doesn't exist, or has new sql
          unless conn.prepared_statements[ps_name] == sql
            conn.execute("DEALLOCATE #{ps_name}") if conn.prepared_statements.include?(ps_name)
            log_yield("PREPARE #{ps_name} AS #{sql}"){conn.prepare(ps_name, sql)}
            conn.prepared_statements[ps_name] = sql
          end

          log_sql = "EXECUTE #{ps_name}"
          if statement.log_sql
            log_sql << " ("
            log_sql << sql
            log_sql << ")"
          end
          log_yield(sql, args) do
            conn.execute_prepared_statement(ps_name, args)
          end
        rescue PGError => e
          if (database_exception_sqlstate(e, opts) == STALE_STATEMENT_SQLSTATE)
            conn.prepared_statements[ps_name] = nil
            retry
          end
        end
      end
    end

    # Dataset class for the FoundationDB SQL Layer that uses the pg driver.
    class Dataset < Sequel::Dataset
      include Sequel::Fdbsql::DatasetMethods

      Database::DatasetClass = self

      # Allow use of bind arguments for FDBSQL using the pg driver.
      module BindArgumentMethods

        include Sequel::Dataset::UnnumberedArgumentMapper
        include DatasetMethods::PreparedStatementMethods

        private

        # Execute the given SQL with the stored bind arguments.
        def execute(sql, opts=OPTS, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end

        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts=OPTS, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end
      end

      # Allow use of server side prepared statements for FDBSQL using the
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
        def execute(sql, opts=OPTS, &block)
          super(prepared_statement_name, opts, &block)
        end

        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts=OPTS, &block)
          super(prepared_statement_name, opts, &block)
        end
      end


      # Execute the given type of statement with the hash of values.
      def call(type, bind_vars=OPTS, *values, &block)
        ps = to_prepared_statement(type, values)
        ps.extend(BindArgumentMethods)
        ps.call(bind_vars, &block)
      end

      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql)
        execute(sql) do |res|
          columns = set_columns(res)
          yield_hash_rows(res, columns) {|h| yield h}
        end
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

      def set_columns(res)
        cols = []
        procs = db.conversion_procs
        res.nfields.times do |fieldnum|
          cols << [fieldnum, procs[res.ftype(fieldnum)], output_identifier(res.fname(fieldnum))]
        end
        @columns = cols.map{|c| c[2]}
        cols
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

    # Connection specific methods for Fdbsql with pg
    class Connection < PG::Connection
      # Regular expression for error messages that note that the connection is closed.
      DISCONNECT_ERROR_RE = /\A(?:could not receive data from server|no connection to the server|connection not open|connection is closed)/

      # Hash of prepared statements for this connection.  Keys are
      # string names of the server side prepared statement, and values
      # are SQL strings.
      attr_accessor :prepared_statements

      # Create a new connection to the FoundationDB SQL Layer. See Database#connect.
      def initialize(db, opts)
        connect_opts = {
          :host => opts[:host] || 'localhost',
          :port => opts[:port] || 15432,
          :dbname => opts[:database],
          :user => opts[:user],
          :password => opts[:password],
          :hostaddr => opts[:hostaddr],
          :connect_timeout => opts[:connect_timeout] || 20,
          :sslmode => opts[:sslmode]
        }.delete_if{|key, value| value.nil? or (value.respond_to?(:empty?) and value.empty?)}
	super(connect_opts)

        @db = db
        @prepared_statements = {}

        if opts[:notice_receiver]
          set_notice_receiver(opts[:notice_receiver])
        else
          # Swallow warnings
          set_notice_receiver{|proc| }
        end
      end

      # Close the connection.
      def close
        super
      rescue PGError, IOError
      end

      # Execute the given SQL with this connection.  If a block is given,
      # yield the results, otherwise, return the number of changed rows.
      def execute(sql, args=nil)
        q = query(sql, args)
        block_given? ? yield(q) : q.cmd_tuples
      end

      # Execute the prepared statement of the given name, binding the given
      # args.
      def execute_prepared_statement(name, args)
        check_disconnect_errors{exec_prepared(name, args)}
      end

      # Prepare a statement for later use.
      def prepare(name, sql)
        check_disconnect_errors{super}
      end

      # Execute the given query and return the results.
      def query(sql, args=nil)
        args = args.map{|v| @db.bound_variable_arg(v, self)} if args
        check_disconnect_errors{super}
      end

      private

      # Raise a Sequel::DatabaseDisconnectError if a PGError is raised and
      # the connection status cannot be determined or it is not OK.
      def check_disconnect_errors
        begin
          yield
        rescue PGError => e
          disconnect = false
          disconnect ||= e.message =~ DISCONNECT_ERROR_RE
          disconnect ? raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError)) : raise
        rescue IOError, Errno::EPIPE, Errno::ECONNRESET => e
          disconnect = true
          raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError))
        end
      end
    end
  end
end
