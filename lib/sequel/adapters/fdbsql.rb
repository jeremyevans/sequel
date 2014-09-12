#
# FoundationDB SQL Layer Sequel Adapter
# Copyright (c) 2013-2014 FoundationDB, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

# FoundationDB SQL Layer currently uses the Postgres protocol
require 'pg'

require 'sequel/adapters/utils/pg_types'
require 'sequel/adapters/shared/fdbsql'

module Sequel

  Database::ADAPTERS << :fdbsql

  def_adapter_method(:fdbsql)

  module Fdbsql
    CONVERTED_EXCEPTIONS << PGError

    class Database < Sequel::Database
      include Sequel::Fdbsql::DatabaseMethods

      set_adapter_scheme :fdbsql

      def connect(server)
        opts = server_opts(server)
        Connection.new(self, opts)
      end

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

      def begin_transaction(conn, opts=OPTS)
        super
        # TODO add functionality to jdbc
        conn.in_transaction = true
      end

      def remove_transaction(conn, committed)
        conn.in_transaction = false
      ensure
        super
      end

    end

    class Dataset < Sequel::Dataset
      include Sequel::Fdbsql::DatasetMethods

      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |res|
          columns = set_columns(res)
          yield_hash_rows(res, columns) {|h| yield h}
        end
      end

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

      def set_columns(res)
        cols = []
        procs = db.conversion_procs
        res.nfields.times do |fieldnum|
          cols << [fieldnum, procs[res.ftype(fieldnum)], output_identifier(res.fname(fieldnum))]
        end
        @columns = cols.map{|c| c[2]}
        cols
      end

    end

    class Connection
      CONNECTION_OK = -1

      DISCONNECT_ERROR_RE = /\A(?:could not receive data from server|no connection to the server|connection not open|connection is closed)/

      # These sql states are used to indicate that fdbvql should automatically
      # retry the statement if it's not in a transaction
      RETRY_SQLSTATES = %w'40002'.freeze.each{|s| s.freeze}

      attr_accessor :in_transaction

      attr_accessor :prepared_statements

      def initialize(db, opts)
        @db = db
        @config = opts
        @connection_hash = {
          :host => @config[:host] || 'localhost',
          :port => @config[:port] || 15432,
          :dbname => @config[:database],
          :user => @config[:user],
          :password => @config[:password],
          :hostaddr => @config[:hostaddr],
          :connect_timeout => @config[:connect_timeout] || 20,
          :sslmode => @config[:sslmode]
        }.delete_if { |key, value| value.nil? or (value.respond_to?(:empty?) and value.empty?)}
        @prepared_statements = {}
        connect
      end

      def close
        # Just like postgres, ignore any errors here
        begin
          @connection.close
        rescue PGError, IOError
        end
      end

      def query(sql, args=nil)
        args = args.map{|v| @db.bound_variable_arg(v, self)} if args
        check_disconnect_errors do
          retry_on_not_committed do
            @connection.query(sql, args)
          end
        end
      end

      # Execute the given SQL with this connection.  If a block is given,
      # yield the results, otherwise, return the number of changed rows.
      def execute(sql, args=nil)
        q = query(sql, args)
        block_given? ? yield(q) : q.cmd_tuples
      end

      def prepare(name, sql)
        check_disconnect_errors do
          retry_on_not_committed do
            @connection.prepare(name, sql)
          end
        end
      end

      def execute_prepared_statement(name, args)
        check_disconnect_errors do
          retry_on_not_committed do
            @connection.exec_prepared(name, args)
          end
        end
      end

      private

      def connect
        @connection = PG::Connection.new(@connection_hash)
        if (@config[:notice_receiver])
          @connection.set_notice_receiver(@config[:notice_receiver])
        else
          # Swallow warnings
          @connection.set_notice_receiver { |proc| }
        end
        check_version
      end

      def check_version
        ver = execute('SELECT VERSION()') do |res|
          version = res.first['_SQL_COL_1']
          m = version.match('^.* (\d+)\.(\d+)\.(\d+)')
          if m.nil?
            raise "No match when checking FDB SQL Layer version: #{version}"
          end
          m
        end

        # Combine into single number, two digits per part: 1.9.3 => 10903
        @sql_layer_version = (100 * ver[1].to_i + ver[2].to_i) * 100 + ver[3].to_i
        if @sql_layer_version < 10906
          raise Sequel::DatabaseError.new("Unsupported FDB SQL Layer version: #{ver[1]}.#{ver[2]}.#{ver[3]}")
        end
      end

      def status
        CONNECTION_OK
      end

      def database_exception_sqlstate(exception, opts)
        if exception.respond_to?(:result) && (result = exception.result)
          result.error_field(::PGresult::PG_DIAG_SQLSTATE)
        end
      end

      def retry_on_not_committed
        retries = NUMBER_OF_NOT_COMMITTED_RETRIES
        begin
          yield
        rescue PG::TRIntegrityConstraintViolation => e
          if (!in_transaction and RETRY_SQLSTATES.include? database_exception_sqlstate(e, :classes=>CONVERTED_EXCEPTIONS))
            retry if (retries -= 1) > 0
          end
          raise
        end
      end

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
          status_ok = (s == CONNECTION_OK)
          disconnect ||= !status_ok
          disconnect ||= e.message =~ DISCONNECT_ERROR_RE
          disconnect ? raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError)) : raise
        rescue IOError, Errno::EPIPE, Errno::ECONNRESET => e
          disconnect = true
          raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError))
        end
      end

    end

    Dataset.register_extension(:date_arithmetic, Sequel::Fdbsql::DateArithmeticDatasetMethods)
  end
end
