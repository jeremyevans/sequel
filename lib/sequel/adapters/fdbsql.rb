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

require 'sequel/adapters/fdbsql/dataset'
require 'sequel/adapters/fdbsql/features'
require 'sequel/adapters/fdbsql/prepared_statements'
require 'sequel/adapters/fdbsql/schema_parsing'
require 'sequel/adapters/utils/pg_types'
require 'sequel/adapters/fdbsql/date_arithmetic'

module Sequel

  Database::ADAPTERS << :fdbsql

  def_adapter_method(:fdbsql)

  module Fdbsql
    CONVERTED_EXCEPTIONS  = [PGError]

    class ExclusionConstraintViolation < Sequel::ConstraintViolation; end
    class RetryError < Sequel::DatabaseError; end
    class NotCommittedError < RetryError; end

    class Database < Sequel::Database
      include DatabaseFeatures
      include DatabasePreparedStatements
      include SchemaParsing
      DatasetClass = Dataset

      # the literal methods put quotes around things, but when we bind a variable there shouldn't be quotes around it
      # it should just be the timestamp, so we need whole new formats here.
      BOUND_VARIABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S".freeze
      BOUND_VARIABLE_SQLTIME_FORMAT = "%H:%M:%S".freeze

      set_adapter_scheme :fdbsql

      attr_reader :conversion_procs

      def adapter_initialize
        @primary_keys = {}
        # Postgres supports named types in the db, if we want to support anything that's not built in, this
        # will have to be changed to not be a constant
        @conversion_procs = Sequel::Postgres::PG_TYPES.dup
        @conversion_procs[16] = Proc.new {|s| s == 'true'}
        @conversion_procs[1184] = @conversion_procs[1114] = method(:to_application_timestamp)
        @conversion_procs.freeze
      end

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

      # Like PostgreSQL fdbsql folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end

      # Like PostgreSQL fdbsql folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      def database_exception_sqlstate(exception, opts)
        if exception.respond_to?(:result) && (result = exception.result)
          result.error_field(::PGresult::PG_DIAG_SQLSTATE)
        end
      end

      def database_error_classes
        [PGError]
      end

      NOT_NULL_CONSTRAINT_SQLSTATES = %w'23502'.freeze.each{|s| s.freeze}
      FOREIGN_KEY_CONSTRAINT_SQLSTATES = %w'23503 23504'.freeze.each{|s| s.freeze}
      UNIQUE_CONSTRAINT_SQLSTATES = %w'23501'.freeze.each{|s| s.freeze}
      NOT_COMMITTED_SQLSTATES = %w'40002'.freeze.each{|s| s.freeze}
      # Given the SQLState, return the appropriate DatabaseError subclass.
      def database_specific_error_class_from_sqlstate(sqlstate)
        # There is also a CheckConstraintViolation in Sequel, but the sql layer doesn't support check constraints
        case sqlstate
        when *NOT_NULL_CONSTRAINT_SQLSTATES
          NotNullConstraintViolation
        when *FOREIGN_KEY_CONSTRAINT_SQLSTATES
          ForeignKeyConstraintViolation
        when *UNIQUE_CONSTRAINT_SQLSTATES
          UniqueConstraintViolation
        when *NOT_COMMITTED_SQLSTATES
          NotCommittedError
        end
      end

      # This is a fallback used by the base class if the sqlstate fails to figure out
      # what error type it is.
      DATABASE_ERROR_REGEXPS = [
        # Add this check first, since otherwise it's possible for users to control
        # which exception class is generated.
        [/invalid input syntax/, DatabaseError],
        # the rest of these are backups in case the sqlstate fails
        [/[dD]uplicate key violates unique constraint/, UniqueConstraintViolation],
        [/due (?:to|for) foreign key constraint/, ForeignKeyConstraintViolation],
        [/NULL value not permitted/, NotNullConstraintViolation],
      ].freeze
      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # Remove the cached entries for primary keys and sequences when a table is
      # changed.
      def remove_cached_schema(table)
        tab = quote_schema_table(table)
        Sequel.synchronize do
          @primary_keys.delete(tab)
        end
        super
      end

      # like PostgreSQL fdbsql uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        {:primary_key => true, :serial => true, :type=>Integer}
      end

      # Add null/not null SQL fragment to column creation SQL.
      # FDBSQL 1.9.5 doesn't implicitly set NOT NULL for primary keys
      # this may be changed in the future, but for now we need to
      # set it at the sequel layer
      def column_definition_null_sql(sql, column)
        null = column.fetch(:null, column[:allow_null])
        sql << NOT_NULL if null == false
        sql << NULL if null == true
      end

      def alter_table_op_sql(table, op)
        quoted_name = quote_identifier(op[:name]) if op[:name]
        case op[:op]
        when :set_column_type
          "ALTER COLUMN #{quoted_name} SET DATA TYPE #{type_literal(op)}"
        when :set_column_null
          "ALTER COLUMN #{quoted_name} #{op[:null] ? '' : 'NOT'} NULL"
        else
          super
        end
      end


      # FDBSQL requires parens around the SELECT, and the WITH DATA syntax.
      def create_table_as_sql(name, sql, options)
        "#{create_table_prefix_sql(name, options)} AS (#{sql}) WITH DATA"
      end

      # Handle bigserial type if :serial option is present
      def type_literal_generic_bignum(column)
        column[:serial] ? :bigserial : super
      end

      # Handle serial type if :serial option is present
      def type_literal_generic_integer(column)
        column[:serial] ? :serial : super
      end

      # Convert given argument so that it can be used directly by pg.  Currently, pg doesn't
      # handle fractional seconds in Time/DateTime or blobs with "\0", and it won't ever
      # handle Sequel::SQLTime values correctly.  Only public for use by the adapter, shouldn't
      # be used by external code.
      def bound_variable_arg(arg, conn)
        case arg
        # TODO TDD it:
        when Sequel::SQL::Blob
          # the 1 means treat this as a binary blob
          {:value => arg, :format => 1}
        when Sequel::SQLTime
          # the literal methods put quotes around things, but this is a bound variable, so we can't use those
          arg.strftime(BOUND_VARIABLE_SQLTIME_FORMAT)
        when DateTime, Time
          # the literal methods put quotes around things, but this is a bound variable, so we can't use those
          from_application_timestamp(arg).strftime(BOUND_VARIABLE_TIMESTAMP_FORMAT)
        else
           arg
        end
      end

      def begin_transaction(conn, opts=OPTS)
        super
        conn.in_transaction = true
      end

      def remove_transaction(conn, committed)
        conn.in_transaction = false
      ensure
        super
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

    end

    class Connection
      CONNECTION_OK = -1
      DISCONNECT_ERROR_RE = /\A(?:could not receive data from server|no connection to the server|connection not open|connection is closed)/

      # These sql states are used to indicate that fdbvql should automatically
      # retry the statement if it's not in a transaction
      RETRY_SQLSTATES = %w'40002'.freeze.each{|s| s.freeze}

      NUMBER_OF_NOT_COMMITTED_RETRIES = 10

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

  end
end
