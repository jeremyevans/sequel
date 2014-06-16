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
require 'sequel/adapters/fdbsql/dataset'
require 'sequel/adapters/fdbsql/create_table_generator'
require 'sequel/adapters/utils/pg_types'
require 'sequel/adapters/fdbsql/date_arithmetic'

module Sequel
  module Fdbsql
    CONVERTED_EXCEPTIONS  = [PGError]

    class ExclusionConstraintViolation < Sequel::ConstraintViolation; end

    class Database < Sequel::Database
      NUMBER_OF_NOT_COMMITTED_RETRIES = 10
      DatasetClass = Dataset
      # Use a PostgreSQL-specific create table generator
      def create_table_generator_class
        CreateTableGenerator
      end

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
      # These sql states are used to indicate that fdbvql should automatically
      # retry the statement if it's not in a transaction
      RETRY_SQLSTATES = %w'40002'.freeze.each{|s| s.freeze}
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

      # Add null/not null SQL fragment to column creation SQL.
      # FDBSQL 1.9.5 doesn't implicitly set NOT NULL for primary keys
      # this may be changed in the future, but for now we need to
      # set it at the sequel layer
      def column_definition_null_sql(sql, column)
        if (column[:primary_key])
          null = false
        else
          null = column.fetch(:null, column[:allow_null])
        end
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
        # TODO bigserial or BGSERIAL, the docs say bgserial, but that seems wrong
        column[:serial] ? :bigserial : super
      end

      # Handle serial type if :serial option is present
      def type_literal_generic_integer(column)
        column[:serial] ? :serial : super
      end


      def schema_parse_table(table_name, opts = {})
        out_identifier = output_identifier_meth(opts[:dataset])
        in_identifier = input_identifier_meth(opts[:dataset])
        # CURRENT_SCHEMA evaluates to the currently chosen schema
        schema = schema ? literal(in_identifier.call(opts[:schema])) : 'CURRENT_SCHEMA'
        # sample output from postgres
        # [:id2, {:oid=>23, :db_type=>"integer", :default=>nil, :allow_null=>false, :primary_key=>true, :type=>:integer}],
        dataset = metadata_dataset.with_sql(<<-EOSQL)
          SELECT c.column_name, (c.is_nullable = 'YES') AS allow_null, c.column_default AS "default", c.data_type AS db_type,
            c.numeric_scale, (tc.constraint_type = 'PRIMARY KEY') AS primary_key
          FROM information_schema.key_column_usage kc
          INNER JOIN information_schema.table_constraints tc
            ON tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = kc.table_name
            AND tc.table_schema = kc.table_schema
            AND tc.constraint_name = kc.constraint_name
          RIGHT JOIN information_schema.columns c
            ON c.table_name = tc.table_name
            AND c.table_schema = tc.table_schema
            AND c.column_name = kc.column_name
          WHERE c.table_name = #{literal(in_identifier.call(table_name.to_s))}
          AND c.table_schema = #{schema}
        EOSQL
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(normalize_decimal_to_integer(row[:db_type], row[:numeric_scale]))
          [out_identifier.call(row.delete(:column_name)), row]
        end
      end

      def column_schema_normalize_default(default, type)
        # some other dbs throw quotes around string-like things,
        # but if there's no quotes sequel just throws the string default out
        # this overrides that to not expect the quotes
        return default
      end

      def normalize_decimal_to_integer(type, scale)
        if (type == 'DECIMAL' and scale == 0)
          'integer'
        else
          type
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
        retries = NUMBER_OF_NOT_COMMITTED_RETRIES
        begin
          yield
        rescue PG::TRIntegrityConstraintViolation => e
          if (!in_transaction? and RETRY_SQLSTATES.include? database_exception_sqlstate(e, :classes=>CONVERTED_EXCEPTIONS))
            retry if (retries -= 1) > 0
          end
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        rescue => e
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        end
      end

    end

  end
end
