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

      STALE_STATEMENT_SQLSTATE = '0A50A'

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

      # indexes are namespaced per table
      def global_index_namespace?
        false
      end

      # Fdbsql supports deferrable fk constraints
      def supports_deferrable_foreign_key_constraints?
        true
      end

      # the sql layer supports CREATE TABLE IF NOT EXISTS syntax,
      def supports_create_table_if_not_exists?
        true
      end

      # the sql layer supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
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

    class Dataset < Sequel::Dataset
      include DatasetFeatures
      include DatasetPreparedStatements

      def fetch_rows(sql)
        execute(sql) do |res|
          columns = set_columns(res)
          yield_hash_rows(res, columns) {|h| yield h}
        end
      end

      Dataset.def_sql_method(self, :delete, %w'with delete from using where returning')
      Dataset.def_sql_method(self, :insert, %w'with insert into columns values returning')
      Dataset.def_sql_method(self, :update, %w'with update table set from where returning')

      # Insert given values into the database.
      def insert(*values)
        if @opts[:returning]
          # Already know which columns to return, let the standard code handle it
          super
        elsif @opts[:sql] || @opts[:disable_insert_returning]
          # Raw SQL used or RETURNING disabled, just use the default behavior
          # and return nil since sequence is not known.
          super
          nil
        else
          # Force the use of RETURNING with the primary key value,
          # unless it has been disabled.
          returning(*insert_pk).insert(*values){|r| return r.values.first}
        end
      end
      PREPARED_ARG_PLACEHOLDER = LiteralString.new('$').freeze

      # FDBSQL specific argument mapper used for mapping the named
      # argument hash to a array with numbered arguments.
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

      # Allow use of bind arguments for FDBSQL using the pg driver.
      module BindArgumentMethods

        include ArgumentMapper

        # Override insert action to use RETURNING if the server supports it.
        def run
          if @prepared_type == :insert
            fetch_rows(prepared_sql){|r| return r.values.first}
          else
            super
          end
        end

        def prepared_sql
          return @prepared_sql if @prepared_sql
          @opts[:returning] = insert_pk if @prepared_type == :insert
          super
          @prepared_sql
        end

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

      # Insert a record returning the record inserted.  Always returns nil without
      # inserting a query if disable_insert_returning is used.
      def insert_select(*values)
        unless @opts[:disable_insert_returning]
          ds = opts[:returning] ? self : returning
          ds.insert(*values){|r| return r}
        end
      end

      # For multiple table support, PostgreSQL requires at least
      # two from tables, with joins allowed.
      def join_from_sql(type, sql)
        if(from = @opts[:from][1..-1]).empty?
          raise(Error, 'Need multiple FROM tables if updating/deleting a dataset with JOINs') if @opts[:join]
        else
          sql << SPACE << type.to_s << SPACE
          source_list_append(sql, from)
          select_join_sql(sql)
        end
      end

      # Use FROM to specify additional tables in an update query
      def update_from_sql(sql)
        join_from_sql(:FROM, sql)
      end

      # Use USING to specify additional tables in a delete query
      def delete_using_sql(sql)
        join_from_sql(:USING, sql)
      end

      # fdbsql does not support FOR UPDATE, because it's unnecessary with the transaction model
      def select_lock_sql(sql)
        @opts[:lock] == :update ? sql : super
      end

      # Emulate the bitwise operators.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :&, :|, :^, :<<, :>>, :'B~'
          complex_expression_emulate_append(sql, op, args)
        # REGEXP_OPERATORS = [:~, :'!~', :'~*', :'!~*']
        when :'~'
          function_sql_append(sql, SQL::Function.new(:REGEX, args.at(0), args.at(1)))
        when :'!~'
          sql << NOT_SPACE
          function_sql_append(sql, SQL::Function.new(:REGEX, args.at(0), args.at(1)))
        when :'~*'
          function_sql_append(sql, SQL::Function.new(:IREGEX, args.at(0), args.at(1)))
        when :'!~*'
          sql << NOT_SPACE
          function_sql_append(sql, SQL::Function.new(:IREGEX, args.at(0), args.at(1)))
        else
          super
        end
      end

      # Append the SQL fragment for the DateAdd expression to the SQL query.
      def date_add_sql_append(sql, da)
        h = da.interval
        expr = da.expr
        interval = ""
        each_valid_interval_unit(h, DEF_DURATION_UNITS) do |value, sql_unit|
          interval << "#{value} #{sql_unit} "
        end
        if interval.empty?
          return literal_append(sql, Sequel.cast(expr, Time))
        else
          return complex_expression_sql_append(sql, :+, [Sequel.cast(expr, Time), Sequel.cast(interval, :interval)])
        end
      end

      # FDBSQL uses a preceding x for hex escaping strings
      def literal_blob_append(sql, v)
        if v.empty?
          sql << "''"
        else
          sql << "x'#{v.unpack('H*').first}'"
        end
      end

      # FDBSQL does: supports_regexp? (but with functions)
      def supports_regexp?
        true
      end

      # Returning is always supported.
      def supports_returning?(type)
        true
      end

      # FDBSQL truncates all seconds
      def supports_timestamp_usecs?
        false
      end

      def supports_quoted_function_names?
        true
      end

      private

      # PostgreSQL uses $N for placeholders instead of ?, so use a $
      # as the placeholder.
      def prepared_arg_placeholder
        PREPARED_ARG_PLACEHOLDER
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

      # Return the primary key to use for RETURNING in an INSERT statement
      def insert_pk
        if (f = opts[:from]) && !f.empty?
          case t = f.first
          when Symbol, String, SQL::Identifier, SQL::QualifiedIdentifier
            if pk = db.primary_key(t)
              pk
            end
          end
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
