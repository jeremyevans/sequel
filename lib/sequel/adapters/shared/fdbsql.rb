Sequel.require 'adapters/utils/pg_types'

module Sequel

  # Top level module for holding all FoundationDB SQL Layer related modules and
  # classes for Sequel.
  module Fdbsql

    # Array of exceptions that need to be converted.  JDBC
    # uses NativeExceptions, the native adapter uses PGError.
    CONVERTED_EXCEPTIONS  = []

    # Methods shared by Database instances that connect to
    # the FoundationDB SQL Layer
    module DatabaseMethods

      # A hash of conversion procs, keyed by type integer (oid) and
      # having callable values for the conversion proc for that type.
      attr_reader :conversion_procs

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

      # Fdbsql uses the :fdbsql database type.
      def database_type
        :fdbsql
      end

      # like PostgreSQL fdbsql uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        {:primary_key => true, :serial => true, :type=>Integer}
      end

      # indexes are namespaced per table
      def global_index_namespace?
        false
      end

      # Return primary key for the given table.
      def primary_key(table_name, opts=OPTS)
        quoted_table = quote_schema_table(table_name)
        Sequel.synchronize{return @primary_keys[quoted_table] if @primary_keys.has_key?(quoted_table)}
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table_name, opts)
        dataset = metadata_dataset.
          select(:kc__column_name).
          from(Sequel.as(:information_schema__key_column_usage, 'kc')).
          join(Sequel.as(:information_schema__table_constraints, 'tc'),
               [:table_name, :table_schema, :constraint_name]).
          where(:kc__table_name => in_identifier.call(table),
                :kc__table_schema => schema,
                :tc__constraint_type => 'PRIMARY KEY')
        value = dataset.map do |row|
          out_identifier.call(row.delete(:column_name))
        end
        value = case value.size
                  when 0 then nil
                  when 1 then value.first
                  else value
                end
        Sequel.synchronize{@primary_keys[quoted_table] = value}
      end

      # the sql layer supports CREATE TABLE IF NOT EXISTS syntax,
      def supports_create_table_if_not_exists?
        true
      end

      # Fdbsql supports deferrable fk constraints
      def supports_deferrable_foreign_key_constraints?
        true
      end

      # the sql layer supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
      end

      # Array of symbols specifying table names in the current database.
      # The dataset used is yielded to the block if one is provided,
      # otherwise, an array of symbols of table names is returned.
      #
      # Options:
      # :qualify :: Return the tables as Sequel::SQL::QualifiedIdentifier instances,
      #             using the schema the table is located in as the qualifier.
      # :schema :: The schema to search
      # :server :: The server to use
      def tables(opts=OPTS, &block)
        tables_or_views('TABLE', opts, &block)
      end

      # Array of symbols specifying view names in the current database.
      #
      # Options:
      # :qualify :: Return the views as Sequel::SQL::QualifiedIdentifier instances,
      #             using the schema the view is located in as the qualifier.
      # :schema :: The schema to search
      # :server :: The server to use
      def views(opts=OPTS, &block)
        tables_or_views('VIEW', opts, &block)
      end

      # Return full foreign key information, including
      # Postgres returns hash like:
      # {"b_e_fkey"=> {:name=>:b_e_fkey, :columns=>[:e], :on_update=>:no_action, :on_delete=>:no_action, :deferrable=>false, :table=>:a, :key=>[:c]}}
      def foreign_key_list(table, opts=OPTS)
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table, opts)
        sql_table = in_identifier.call(table)
        columns_dataset = metadata_dataset.
          select(:tc__table_name___table_name,
                 :tc__table_schema___table_schema,
                 :tc__is_deferrable___deferrable,
                 :kc__column_name___column_name,
                 :kc__constraint_schema___schema,
                 :kc__constraint_name___name,
                 :rc__update_rule___on_update,
                 :rc__delete_rule___on_delete).
          from(Sequel.as(:information_schema__table_constraints, 'tc')).
          join(Sequel.as(:information_schema__key_column_usage, 'kc'),
               [:constraint_schema, :constraint_name]).
          join(Sequel.as(:information_schema__referential_constraints, 'rc'),
               [:constraint_name, :constraint_schema]).
          where(:tc__table_name => sql_table,
                :tc__table_schema => schema,
                :tc__constraint_type => 'FOREIGN KEY')

        keys_dataset = metadata_dataset.
          select(:rc__constraint_schema___schema,
                 :rc__constraint_name___name,
                 :kc__table_name___key_table,
                 :kc__column_name___key_column).
          from(Sequel.as(:information_schema__table_constraints, 'tc')).
          join(Sequel.as(:information_schema__referential_constraints, 'rc'),
               [:constraint_schema, :constraint_name]).
          join(Sequel.as(:information_schema__key_column_usage, 'kc'),
               :kc__constraint_schema => :rc__unique_constraint_schema,
               :kc__constraint_name => :rc__unique_constraint_name).
          where(:tc__table_name => sql_table,
                :tc__table_schema => schema,
                :tc__constraint_type => 'FOREIGN KEY')
        foreign_keys = {}
        columns_dataset.each do |row|
          foreign_key = foreign_keys.fetch(row[:name]) do |key|
            foreign_keys[row[:name]] = row
            row[:name] = out_identifier.call(row[:name])
            row[:columns] = []
            row[:key] = []
            row
          end
          foreign_key[:columns] << out_identifier.call(row[:column_name])
        end
        keys_dataset.each do |row|
          foreign_key = foreign_keys[row[:name]]
          foreign_key[:table] = out_identifier.call(row[:key_table])
          foreign_key[:key] << out_identifier.call(row[:key_column])
        end
        foreign_keys.values
      end

      # Return indexes for the table
      # postgres returns:
      # {:blah_blah_index=>{:columns=>[:n], :unique=>true, :deferrable=>nil},
      #  :items_n_a_index=>{:columns=>[:n, :a], :unique=>false, :deferrable=>nil}}
      def indexes(table, opts=OPTS)
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table, opts)
        dataset = metadata_dataset.
          select(:is__is_unique,
                 Sequel.as({:is__is_unique => 'YES'}, 'unique'),
                 :is__index_name,
                 :ic__column_name).
          from(Sequel.as(:information_schema__indexes, 'is')).
          join(Sequel.as(:information_schema__index_columns, 'ic'),
               :ic__index_table_schema => :is__table_schema,
               :ic__index_table_name => :is__table_name,
               :ic__index_name => :is__index_name).
          where(:is__table_schema => schema,
                :is__table_name => in_identifier.call(table)).
          exclude(:is__index_type => 'PRIMARY')
        indexes = {}
        dataset.each do |row|
          index = indexes.fetch(out_identifier.call(row[:index_name])) do |key|
            h = { :unique => row[:unique], :columns => [] }
            indexes[key] = h
            h
          end
          index[:columns] << out_identifier.call(row[:column_name])
        end
        indexes
      end

      private

      # the literal methods put quotes around things, but when we bind a variable there shouldn't be quotes around it
      # it should just be the timestamp, so we need whole new formats here.
      BOUND_VARIABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S".freeze
      BOUND_VARIABLE_SQLTIME_FORMAT = "%H:%M:%S".freeze

      def adapter_initialize
        @primary_keys = {}
        # Postgres supports named types in the db, if we want to support anything that's not built in, this
        # will have to be changed to not be a constant
        @conversion_procs = Sequel::Postgres::PG_TYPES.dup
        @conversion_procs[16] = Proc.new {|s| s == 'true'}
        @conversion_procs[1184] = @conversion_procs[1114] = method(:to_application_timestamp)
        @conversion_procs.freeze
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

      # Convert exceptions raised from the block into DatabaseErrors.
      def check_database_errors
        begin
          yield
        rescue => e
          raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
        end
      end

      def column_schema_normalize_default(default, type)
        # the default value returned by schema parsing is not escaped or quoted
        # in any way, it's just the value of the string
        # the base implementation assumes it would come back "'my ''default'' value'"
        # fdbsql returns "my 'default' value" (Not including double quotes for either)
        return default
      end

      # FDBSQL requires parens around the SELECT, and the WITH DATA syntax.
      def create_table_as_sql(name, sql, options)
        "#{create_table_prefix_sql(name, options)} AS (#{sql}) WITH DATA"
      end

      def database_error_classes
        CONVERTED_EXCEPTIONS
      end

      STALE_STATEMENT_SQLSTATE = '0A50A'.freeze
      NOT_NULL_CONSTRAINT_SQLSTATES = %w'23502'.freeze.each{|s| s.freeze}
      FOREIGN_KEY_CONSTRAINT_SQLSTATES = %w'23503 23504'.freeze.each{|s| s.freeze}
      UNIQUE_CONSTRAINT_SQLSTATES = %w'23501'.freeze.each{|s| s.freeze}

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

      def identifier_convertors(opts=OPTS)
        [output_identifier_meth(opts[:dataset]), input_identifier_meth(opts[:dataset])]
      end

      # Like PostgreSQL fdbsql folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end

      # Like PostgreSQL fdbsql folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      # If the given type is DECIMAL with scale 0, say that it's an integer
      def normalize_decimal_to_integer(type, scale)
        if (type == 'DECIMAL' and scale == 0)
          'integer'
        else
          type
        end
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

      def schema_or_current_and_table(table, opts=OPTS)
        schema, table = schema_and_table(table)
        schema = opts.fetch(:schema, schema || Sequel.lit('CURRENT_SCHEMA'))
        [schema, table]
      end

      # returns an array of column information with each column being of the form:
      # [:column_name, {:db_type=>"integer", :default=>nil, :allow_null=>false, :primary_key=>true, :type=>:integer}]
      def schema_parse_table(table, opts = {})
        out_identifier, in_identifier = identifier_convertors(opts)
        schema, table = schema_or_current_and_table(table, opts)
        dataset = metadata_dataset.
          select(:c__column_name,
                 Sequel.as({:c__is_nullable => 'YES'}, 'allow_null'),
                 :c__column_default___default,
                 :c__data_type___db_type,
                 :c__character_maximum_length___max_length,
                 :c__numeric_scale,
                 Sequel.as({:tc__constraint_type => 'PRIMARY KEY'}, 'primary_key')).
          from(Sequel.as(:information_schema__key_column_usage, 'kc')).
          join(Sequel.as(:information_schema__table_constraints, 'tc'),
               :tc__constraint_type => 'PRIMARY KEY',
               :tc__table_name => :kc__table_name,
               :tc__table_schema => :kc__table_schema,
               :tc__constraint_name => :kc__constraint_name).
          right_outer_join(Sequel.as(:information_schema__columns, 'c'),
                           [:table_name, :table_schema, :column_name]).
          where(:c__table_name => in_identifier.call(table),
                :c__table_schema => schema)
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(normalize_decimal_to_integer(row[:db_type], row[:numeric_scale]))
          [out_identifier.call(row.delete(:column_name)), row]
        end
      end

      def tables_or_views(type, opts, &block)
        schema = opts[:schema] || Sequel.lit('CURRENT_SCHEMA')
        m = output_identifier_meth
        dataset = metadata_dataset.server(opts[:server]).select(:table_name).
          from(Sequel.qualify('information_schema','tables')).
          where(:table_schema => schema,
                :table_type => type)
        if block_given?
          yield(dataset)
        elsif opts[:qualify]
          dataset.select_append(:table_schema).map{|r| Sequel.qualify(m.call(r[:table_schema]), m.call(r[:table_name])) }
        else
          dataset.map{|r| m.call(r[:table_name])}
        end
      end

      # Handle bigserial type if :serial option is present
      def type_literal_generic_bignum(column)
        column[:serial] ? :bigserial : super
      end

      # Handle serial type if :serial option is present
      def type_literal_generic_integer(column)
        column[:serial] ? :serial : super
      end

    end

    # Instance methods for datasets that connect to the FoundationDB SQL Layer.
    module DatasetMethods

      Dataset.def_sql_method(self, :delete, %w'with delete from using where returning')
      Dataset.def_sql_method(self, :insert, %w'with insert into columns values returning')
      Dataset.def_sql_method(self, :update, %w'with update table set from where returning')

      # Shared methods for prepared statements used with the FoundationDB SQL Layer
      module PreparedStatementMethods

        def prepared_sql
          return @prepared_sql if @prepared_sql
          @opts[:returning] = insert_pk if @prepared_type == :insert
          super
          @prepared_sql
        end

        # Override insert action to use RETURNING if the server supports it.
        def run
          if @prepared_type == :insert
            fetch_rows(prepared_sql){|r| return r.values.first}
          else
            super
          end
        end
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
          sql << Sequel::Dataset::NOT_SPACE
          function_sql_append(sql, SQL::Function.new(:REGEX, args.at(0), args.at(1)))
        when :'~*'
          function_sql_append(sql, SQL::Function.new(:IREGEX, args.at(0), args.at(1)))
        when :'!~*'
          sql << Sequel::Dataset::NOT_SPACE
          function_sql_append(sql, SQL::Function.new(:IREGEX, args.at(0), args.at(1)))
        else
          super
        end
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
          returning(*insert_pk).insert(*values){|r| return r.values.first}
        end
      end

      # Insert a record returning the record inserted.  Always returns nil without
      # inserting a query if disable_insert_returning is used.
      def insert_select(*values)
        unless @opts[:disable_insert_returning]
          ds = opts[:returning] ? self : returning
          ds.insert(*values){|r| return r}
        end
      end

      # The SQL to use for an insert_select, adds a RETURNING clause to the insert
      # unless the RETURNING clause is already present.
      def insert_select_sql(*values)
        ds = opts[:returning] ? self : returning
        ds.insert_sql(*values)
      end

      # FDBSQL has functions to support regular expression pattern matching.
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

      # FDBSQL supports quoted function names
      def supports_quoted_function_names?
        true
      end

      private

      # Use USING to specify additional tables in a delete query
      def delete_using_sql(sql)
        join_from_sql(:USING, sql)
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

      # FDBSQL uses a preceding x for hex escaping strings
      def literal_blob_append(sql, v)
        if v.empty?
          sql << "''"
        else
          sql << "x'#{v.unpack('H*').first}'"
        end
      end

      # fdbsql does not support FOR UPDATE, because it's unnecessary with the transaction model
      def select_lock_sql(sql)
        @opts[:lock] == :update ? sql : super
      end

      # Use FROM to specify additional tables in an update query
      def update_from_sql(sql)
        join_from_sql(:FROM, sql)
      end
    end
  end
end
