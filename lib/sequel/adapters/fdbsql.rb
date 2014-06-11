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


require 'sequel/adapters/fdbsql/connection'
require 'sequel/adapters/utils/pg_types'

module Sequel
  module Fdbsql
    CONVERTED_EXCEPTIONS  = [PGError]

    class ExclusionConstraintViolation < Sequel::ConstraintViolation; end

    class Database < Sequel::Database

      set_adapter_scheme :fdbsql

      def adapter_initialize
        @primary_keys = {}
      end

      def connect(server)
        opts = server_opts(server)
        Connection.new(apply_default_options(opts))
      end

      def execute(sql, opts = {}, &block)
        res = nil
        synchronize(opts[:server]) do |conn|
          res = log_yield(sql) do
            check_database_errors do
              conn.query(sql)
            end
          end
          yield res if block_given?
          res.cmd_tuples
        end
      end

      # the sql layer supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
      end

      def supports_schema_parsing?
        true
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


      NOT_NULL_CONSTRAINT_SQLSTATES = %w'23502'.freeze.each{|s| s.freeze}
      FOREIGN_KEY_CONSTRAINT_SQLSTATES = %w'23503 23504'.freeze.each{|s| s.freeze}
      UNIQUE_CONSTRAINT_SQLSTATES = %w'23501'.freeze.each{|s| s.freeze}
      SERIALIZATION_CONSTRAINT_SQLSTATES = %w'40001'.freeze.each{|s| s.freeze}
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
        when *SERIALIZATION_CONSTRAINT_SQLSTATES
          SerializationFailure
        end
      end

      DATABASE_ERROR_REGEXPS = [
        # Add this check first, since otherwise it's possible for users to control
        # which exception class is generated.
        [/invalid input syntax/, DatabaseError],
        # the rest of these are backups in case the sqlstate fails
        [/[dD]uplicate key violates unique constraint/, UniqueConstraintViolation],
        [/due to foreign key constraint/, ForeignKeyConstraintViolation],
        [/violates not-null constraint/, NotNullConstraintViolation],
        [/conflicting key value violates exclusion constraint/, ExclusionConstraintViolation],
        [/could not serialize access/, SerializationFailure],
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

      # Return primary key for the given table.
      def primary_key(table_name, opts=OPTS)
        # CURRENT_SCHEMA evaluates to the currently chosen schema
        quoted_table = quote_schema_table(table_name)
        Sequel.synchronize{return @primary_keys[quoted_table] if @primary_keys.has_key?(quoted_table)}
        schema = schema ? literal(opts[:schema]) : 'CURRENT_SCHEMA'
        sql =
          'SELECT kc.column_name ' +
          'FROM information_schema.table_constraints tc ' +
          'INNER JOIN information_schema.key_column_usage kc ' +
          '  ON  tc.table_schema = kc.table_schema ' +
          '  AND tc.table_name = kc.table_name ' +
          '  AND tc.constraint_name = kc.constraint_name ' +
          'LEFT JOIN information_schema.columns c ' +
          '  ON kc.table_schema = c.table_schema ' +
          '  AND kc.table_name = c.table_name ' +
          '  AND kc.column_name = c.column_name ' +
          "WHERE tc.table_schema = #{schema} " +
          # Symbols get quoted with double quotes, strings get quoted with single quotes
          # since this is a column value, we want to ensure that it's a string
          "  AND tc.table_name = #{literal(table_name.to_s)} " +
          "  AND tc.constraint_type = 'PRIMARY KEY' "
        value = fetch(sql).single_value
        Sequel.synchronize{@primary_keys[quoted_table] = value}
      end

      # like PostgreSQL fdbsql uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        {:primary_key => true, :serial => true, :type=>Integer}
      end

      # Handle bigserial type if :serial option is present
      def type_literal_generic_bignum(column)
        # TODO bigserial or BGSERIAL, the docs say bgserial, but that seems wrong
        column[:serial] ? :bigserial : super
      end

      # Handle serial type if :serial option is present
      def type_literal_generic_integer(column)
        column[:serial] ? :serial : super
      end


      def schema_parse_table(table_name, options = {})
        # CURRENT_SCHEMA evaluates to the currently chosen schema
        schema = schema ? literal(options[:schema]) : 'CURRENT_SCHEMA'

        dataset = metadata_dataset.with_sql(
                                            'SELECT column_name, is_nullable AS allow_null, column_default AS "default", data_type AS db_type ' +
                                            'FROM information_schema.columns ' +
                                            # Symbols get quoted with double quotes, strings get quoted with single quotes
                                            # since this is a column value, we want to ensure that it's a string
                                            "WHERE table_name = #{literal(table_name.to_s)} " +
                                            "AND table_schema = #{schema} ")
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          [row.delete(:column_name).to_sym, row]
        end
      end


      private


      CONNECTION_DEFAULTS = {
        :host => 'localhost',
        :port => 15432,
        :username => 'fdbsql',
        :password => '',
      }

      def apply_default_options(sequel_options)
        config = CONNECTION_DEFAULTS.merge(sequel_options)
        config[:encoding] =
          config[:charset] || 'UTF8'    unless config[:encoding]

        if config.key?(:database)
          database = config[:database]
        else
          raise ArgumentError, "No database specified. Missing config option: database"
        end

        return config
      end

      # Convert exceptions raised from the block into DatabaseErrors.
      def check_database_errors
        begin
          yield
        rescue => e
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        end
      end

    end


    # TODO put in own file if this becomes more than 5 lines
    class Dataset < Sequel::Dataset
      Sequel::Fdbsql::Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |res|
          columns = set_columns(res)
          yield_hash_rows(res, columns) {|h| yield h}
        end
      end

      Dataset.def_sql_method(self, :insert, %w'with insert into columns values returning')

      # Returning is always supported.
      def supports_returning?(type)
        true
      end

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
          returning(insert_pk).insert(*values){|r| return r.values.first}
        end
      end

      # Insert a record returning the record inserted.  Always returns nil without
      # inserting a query if disable_insert_returning is used.
      def insert_select(*values)
        returning.insert(*values){|r| return r} unless @opts[:disable_insert_returning]
      end

      private


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
        procs = Sequel::Postgres::PG_TYPES
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
              Sequel::SQL::Identifier.new(pk)
            end
          end
        end
      end

    end
  end
end
