# frozen-string-literal: true

require_relative '../utils/unmodified_identifiers'

module Sequel
  # Top level module for holding all PostgreSQL-related modules and classes
  # for Sequel.  All adapters that connect to PostgreSQL support the following options:
  #
  # :client_min_messages :: Change the minimum level of messages that PostgreSQL will send to the
  #                         the client.  The PostgreSQL default is NOTICE, the Sequel default is
  #                         WARNING.  Set to nil to not change the server default. Overridable on
  #                         a per instance basis via the :client_min_messages option.
  # :force_standard_strings :: Set to false to not force the use of standard strings.  Overridable
  #                            on a per instance basis via the :force_standard_strings option.
  # :search_path :: Set the schema search_path for this Database's connections.
  #                 Allows to to set which schemas do not need explicit
  #                 qualification, and in which order to check the schemas when
  #                 an unqualified object is referenced.
  module Postgres
    Sequel::Database.set_shared_adapter_scheme(:postgres, self)

    # Exception class ranged when literalizing integers outside the bigint/int8 range.
    class IntegerOutsideBigintRange < InvalidValue; end

    NAN             = 0.0/0.0
    PLUS_INFINITY   = 1.0/0.0
    MINUS_INFINITY  = -1.0/0.0

    boolean = Object.new
    def boolean.call(s) s == 't' end
    integer = Object.new
    def integer.call(s) s.to_i end
    float = Object.new
    def float.call(s) 
      case s
      when 'NaN'
        NAN
      when 'Infinity'
        PLUS_INFINITY
      when '-Infinity'
        MINUS_INFINITY
      else
        s.to_f 
      end
    end
    date = Object.new
    def date.call(s) ::Date.new(*s.split('-').map(&:to_i)) end
    TYPE_TRANSLATOR_DATE = date.freeze
    bytea = Object.new
    def bytea.call(str)
      str = if str.start_with?('\\x')
        # PostgreSQL 9.0+ bytea hex format
        str[2..-1].gsub(/(..)/){|s| s.to_i(16).chr}
      else
        # Historical PostgreSQL bytea escape format
        str.gsub(/\\(\\|'|[0-3][0-7][0-7])/) {|s|
          if s.size == 2 then s[1,1] else s[1,3].oct.chr end
        }
      end
      ::Sequel::SQL::Blob.new(str)
    end

    CONVERSION_PROCS = {}

    {
      [16] => boolean,
      [17] => bytea,
      [20, 21, 23, 26] => integer,
      [700, 701] => float,
      [1700] => ::Kernel.method(:BigDecimal),
      [1083, 1266] => ::Sequel.method(:string_to_time),
      [1082] => ::Sequel.method(:string_to_date),
      [1184, 1114] => ::Sequel.method(:database_to_application_timestamp),
    }.each do |k,v|
      k.each do |n|
        CONVERSION_PROCS[n] = v
      end
    end
    CONVERSION_PROCS.freeze

    module MockAdapterDatabaseMethods
      def bound_variable_arg(arg, conn)
        arg
      end

      def primary_key(table)
        :id
      end

      private

      # Handle NoMethodErrors when parsing schema due to output_identifier
      # being called with nil when the Database fetch results are not set
      # to what schema parsing expects.
      def schema_parse_table(table, opts=OPTS)
        super
      rescue NoMethodError
        []
      end
    end

    def self.mock_adapter_setup(db)
      db.instance_exec do
        @server_version = 170000
        initialize_postgres_adapter
        extend(MockAdapterDatabaseMethods)
      end
    end

    class CreateTableGenerator < Sequel::Schema::CreateTableGenerator
      # Add an exclusion constraint when creating the table. Elements should be
      # an array of 2 element arrays, with the first element being the column or
      # expression the exclusion constraint is applied to, and the second element
      # being the operator to use for the column/expression to check for exclusion:
      #
      #   exclude([[:col1, '&&'], [:col2, '=']])
      #   # EXCLUDE USING gist (col1 WITH &&, col2 WITH =)
      #
      # To use a custom operator class, you need to use Sequel.lit with the expression
      # and operator class:
      #
      #   exclude([[Sequel.lit('col1 inet_ops'), '&&'], [:col2, '=']])
      #   # EXCLUDE USING gist (col1 inet_ops WITH &&, col2 WITH =)
      #
      # Options supported:
      #
      # :include :: Include additional columns in the underlying index, to
      #             allow for index-only scans in more cases (PostgreSQL 11+).
      # :name :: Name the constraint with the given name (useful if you may
      #          need to drop the constraint later)
      # :using :: Override the index_method for the exclusion constraint (defaults to gist).
      # :where :: Create a partial exclusion constraint, which only affects
      #           a subset of table rows, value should be a filter expression.
      def exclude(elements, opts=OPTS)
        constraints << {:type => :exclude, :elements => elements}.merge!(opts)
      end
    end

    class AlterTableGenerator < Sequel::Schema::AlterTableGenerator
      # Adds an exclusion constraint to an existing table, see
      # CreateTableGenerator#exclude.
      def add_exclusion_constraint(elements, opts=OPTS)
        @operations << {:op => :add_constraint, :type => :exclude, :elements => elements}.merge!(opts)
      end

      # Alter an existing constraint.  Options:
      # :deferrable :: Modify deferrable setting for constraint (PostgreSQL 9.4+):
      #                true :: DEFERRABLE INITIALLY DEFERRED
      #                false :: NOT DEFERRABLE
      #                :immediate :: DEFERRABLE INITIALLY IMMEDIATE
      # :enforced :: Set true to use ENFORCED, or false to use NOT ENFORCED (PostgreSQL 18+)
      # :inherit :: Set true to use INHERIT, or false to use NO INHERIT (PostgreSQL 18+)
      def alter_constraint(name, opts=OPTS)
        @operations << {:op => :alter_constraint, :name => name}.merge!(opts)
      end

      # Validate the constraint with the given name, which should have
      # been added previously with NOT VALID.
      def validate_constraint(name)
        @operations << {:op => :validate_constraint, :name => name}
      end
    end

    # Generator used for creating tables that are partitions of other tables.
    class CreatePartitionOfTableGenerator
      MINVALUE = Sequel.lit('MINVALUE').freeze
      MAXVALUE = Sequel.lit('MAXVALUE').freeze

      def initialize(&block)
        instance_exec(&block)
      end

      # The minimum value of the data type used in range partitions, useful
      # as an argument to #from.
      def minvalue
        MINVALUE
      end

      # The minimum value of the data type used in range partitions, useful
      # as an argument to #to.
      def maxvalue
        MAXVALUE
      end

      # Assumes range partitioning, sets the inclusive minimum value of the range for
      # this partition.
      def from(*v)
        @from = v
      end

      # Assumes range partitioning, sets the exclusive maximum value of the range for
      # this partition.
      def to(*v)
        @to = v
      end

      # Assumes list partitioning, sets the values to be included in this partition.
      def values_in(*v)
        @in = v
      end

      # Assumes hash partitioning, sets the modulus for this parition.
      def modulus(v)
        @modulus = v
      end

      # Assumes hash partitioning, sets the remainder for this parition.
      def remainder(v)
        @remainder = v
      end

      # Sets that this is a default partition, where values not in other partitions
      # are stored.
      def default
        @default = true
      end

      # The from and to values of this partition for a range partition.
      def range
        [@from, @to]
      end

      # The values to include in this partition for a list partition.
      def list
        @in
      end

      # The modulus and remainder to use for this partition for a hash partition.
      def hash_values
        [@modulus, @remainder]
      end

      # Determine the appropriate partition type for this partition by which methods
      # were called on it.
      def partition_type
        raise Error, "Unable to determine partition type, multiple different partitioning methods called" if [@from || @to, @list, @modulus || @remainder, @default].compact.length > 1

        if @from || @to
          raise Error, "must call both from and to when creating a partition of a table if calling either" unless @from && @to
          :range
        elsif @in
          :list
        elsif @modulus || @remainder
          raise Error, "must call both modulus and remainder when creating a partition of a table if calling either" unless @modulus && @remainder
          :hash
        elsif @default
          :default
        else
          raise Error, "unable to determine partition type, no partitioning methods called"
        end
      end
    end

    # Error raised when Sequel determines a PostgreSQL exclusion constraint has been violated.
    class ExclusionConstraintViolation < Sequel::ConstraintViolation; end

    module DatabaseMethods
      include UnmodifiedIdentifiers::DatabaseMethods

      FOREIGN_KEY_LIST_ON_DELETE_MAP = {'a'=>:no_action, 'r'=>:restrict, 'c'=>:cascade, 'n'=>:set_null, 'd'=>:set_default}.freeze
      ON_COMMIT = {:drop => 'DROP', :delete_rows => 'DELETE ROWS', :preserve_rows => 'PRESERVE ROWS'}.freeze
      ON_COMMIT.each_value(&:freeze)

      # SQL fragment for custom sequences (ones not created by serial primary key),
      # Returning the schema and literal form of the sequence name, by parsing
      # the column defaults table.
      SELECT_CUSTOM_SEQUENCE_SQL = (<<-end_sql
        SELECT name.nspname AS "schema",
            CASE
            WHEN split_part(pg_get_expr(def.adbin, attr.attrelid), '''', 2) ~ '.' THEN
              substr(split_part(pg_get_expr(def.adbin, attr.attrelid), '''', 2),
                     strpos(split_part(pg_get_expr(def.adbin, attr.attrelid), '''', 2), '.')+1)
            ELSE split_part(pg_get_expr(def.adbin, attr.attrelid), '''', 2)
          END AS "sequence"
        FROM pg_class t
        JOIN pg_namespace  name ON (t.relnamespace = name.oid)
        JOIN pg_attribute  attr ON (t.oid = attrelid)
        JOIN pg_attrdef    def  ON (adrelid = attrelid AND adnum = attnum)
        JOIN pg_constraint cons ON (conrelid = adrelid AND adnum = conkey[1])
        WHERE cons.contype = 'p'
          AND pg_get_expr(def.adbin, attr.attrelid) ~* 'nextval'
      end_sql
      ).strip.gsub(/\s+/, ' ').freeze # SEQUEL6: Remove

      # SQL fragment for determining primary key column for the given table.  Only
      # returns the first primary key if the table has a composite primary key.
      SELECT_PK_SQL = (<<-end_sql
        SELECT pg_attribute.attname AS pk
        FROM pg_class, pg_attribute, pg_index, pg_namespace
        WHERE pg_class.oid = pg_attribute.attrelid
          AND pg_class.relnamespace  = pg_namespace.oid
          AND pg_class.oid = pg_index.indrelid
          AND pg_index.indkey[0] = pg_attribute.attnum
          AND pg_index.indisprimary = 't'
      end_sql
      ).strip.gsub(/\s+/, ' ').freeze # SEQUEL6: Remove

      # SQL fragment for getting sequence associated with table's
      # primary key, assuming it was a serial primary key column.
      SELECT_SERIAL_SEQUENCE_SQL = (<<-end_sql
        SELECT  name.nspname AS "schema", seq.relname AS "sequence"
        FROM pg_class seq, pg_attribute attr, pg_depend dep,
          pg_namespace name, pg_constraint cons, pg_class t
        WHERE seq.oid = dep.objid
          AND seq.relnamespace  = name.oid
          AND seq.relkind = 'S'
          AND attr.attrelid = dep.refobjid
          AND attr.attnum = dep.refobjsubid
          AND attr.attrelid = cons.conrelid
          AND attr.attnum = cons.conkey[1]
          AND attr.attrelid = t.oid
          AND cons.contype = 'p'
      end_sql
      ).strip.gsub(/\s+/, ' ').freeze # SEQUEL6: Remove

      # A hash of conversion procs, keyed by type integer (oid) and
      # having callable values for the conversion proc for that type.
      attr_reader :conversion_procs

      # Set a conversion proc for the given oid.  The callable can
      # be passed either as a argument or a block.
      def add_conversion_proc(oid, callable=nil, &block)
        conversion_procs[oid] = callable || block
      end

      # Add a conversion proc for a named type, using the given block.
      # This should be used for types without fixed OIDs, which includes all types that
      # are not included in a default PostgreSQL installation.
      def add_named_conversion_proc(name, &block)
        unless oid = from(:pg_type).where(:typtype=>['b', 'e'], :typname=>name.to_s).get(:oid)
          raise Error, "No matching type in pg_type for #{name.inspect}"
        end
        add_conversion_proc(oid, block)
      end

      def commit_prepared_transaction(transaction_id, opts=OPTS)
        run("COMMIT PREPARED #{literal(transaction_id)}", opts)
      end

      # A hash of metadata for CHECK constraints on the table.
      # Keys are CHECK constraint name symbols.  Values are hashes with the following keys:
      # :definition :: An SQL fragment for the definition of the constraint
      # :columns :: An array of column symbols for the columns referenced in the constraint,
      #             can be an empty array if the database cannot deteremine the column symbols.
      def check_constraints(table)
        m = output_identifier_meth

        hash = {}
        _check_constraints_ds.where_each(:conrelid=>regclass_oid(table)) do |row|
          constraint = m.call(row[:constraint])
          entry = hash[constraint] ||= {:definition=>row[:definition], :columns=>[], :validated=>row[:validated], :enforced=>row[:enforced]}
          entry[:columns] << m.call(row[:column]) if row[:column]
        end
        
        hash
      end

      # Convert the first primary key column in the +table+ from being a serial column to being an identity column.
      # If the column is already an identity column, assume it was already converted and make no changes.
      #
      # Only supported on PostgreSQL 10.2+, since on those versions Sequel will use identity columns
      # instead of serial columns for auto incrementing primary keys. Only supported when running as
      # a superuser, since regular users cannot modify system tables, and there is no way to keep an
      # existing sequence when changing an existing column to be an identity column.
      #
      # This method can raise an exception in at least the following cases where it may otherwise succeed
      # (there may be additional cases not listed here):
      #
      # * The serial column was added after table creation using PostgreSQL <7.3
      # * A regular index also exists on the column (such an index can probably be dropped as the
      #   primary key index should suffice)
      #
      # Options:
      # :column :: Specify the column to convert instead of using the first primary key column
      # :server :: Run the SQL on the given server
      def convert_serial_to_identity(table, opts=OPTS)
        raise Error, "convert_serial_to_identity is only supported on PostgreSQL 10.2+" unless server_version >= 100002

        server = opts[:server]
        server_hash = server ? {:server=>server} : OPTS
        ds = dataset
        ds = ds.server(server) if server

        raise Error, "convert_serial_to_identity requires superuser permissions" unless ds.get{current_setting('is_superuser')} == 'on'

        table_oid = regclass_oid(table)
        im = input_identifier_meth
        unless column = (opts[:column] || ((sch = schema(table).find{|_, sc| sc[:primary_key] && sc[:auto_increment]}) && sch[0]))
          raise Error, "could not determine column to convert from serial to identity automatically"
        end
        column = im.call(column)

        column_num = ds.from(:pg_attribute).
          where(:attrelid=>table_oid, :attname=>column).
          get(:attnum)

        pg_class = Sequel.cast('pg_class', :regclass)
        res = ds.from(:pg_depend).
          where(:refclassid=>pg_class, :refobjid=>table_oid, :refobjsubid=>column_num, :classid=>pg_class, :objsubid=>0, :deptype=>%w'a i').
          select_map([:objid, Sequel.as({:deptype=>'i'}, :v)])

        case res.length
        when 0
          raise Error, "unable to find related sequence when converting serial to identity"
        when 1
          seq_oid, already_identity = res.first
        else
          raise Error, "more than one linked sequence found when converting serial to identity"
        end

        return if already_identity

        transaction(server_hash) do
          run("ALTER TABLE #{quote_schema_table(table)} ALTER COLUMN #{quote_identifier(column)} DROP DEFAULT", server_hash)

          ds.from(:pg_depend).
            where(:classid=>pg_class, :objid=>seq_oid, :objsubid=>0, :deptype=>'a').
            update(:deptype=>'i')

          ds.from(:pg_attribute).
            where(:attrelid=>table_oid, :attname=>column).
            update(:attidentity=>'d')
        end

        remove_cached_schema(table)
        nil
      end

      # Creates the function in the database.  Arguments:
      # name :: name of the function to create
      # definition :: string definition of the function, or object file for a dynamically loaded C function.
      # opts :: options hash:
      #         :args :: function arguments, can be either a symbol or string specifying a type or an array of 1-3 elements:
      #                  1 :: argument data type
      #                  2 :: argument name
      #                  3 :: argument mode (e.g. in, out, inout)
      #         :behavior :: Should be IMMUTABLE, STABLE, or VOLATILE.  PostgreSQL assumes VOLATILE by default.
      #         :parallel :: The thread safety attribute of the function. Should be SAFE, UNSAFE, RESTRICTED. PostgreSQL assumes UNSAFE by default.
      #         :cost :: The estimated cost of the function, used by the query planner.
      #         :language :: The language the function uses.  SQL is the default.
      #         :link_symbol :: For a dynamically loaded see function, the function's link symbol if different from the definition argument.
      #         :returns :: The data type returned by the function.  If you are using OUT or INOUT argument modes, this is ignored.
      #                     Otherwise, if this is not specified, void is used by default to specify the function is not supposed to return a value.
      #         :rows :: The estimated number of rows the function will return.  Only use if the function returns SETOF something.
      #         :security_definer :: Makes the privileges of the function the same as the privileges of the user who defined the function instead of
      #                              the privileges of the user who runs the function.  There are security implications when doing this, see the PostgreSQL documentation.
      #         :set :: Configuration variables to set while the function is being run, can be a hash or an array of two pairs.  search_path is
      #                 often used here if :security_definer is used.
      #         :strict :: Makes the function return NULL when any argument is NULL.
      def create_function(name, definition, opts=OPTS)
        self << create_function_sql(name, definition, opts)
      end

      # Create the procedural language in the database. Arguments:
      # name :: Name of the procedural language (e.g. plpgsql)
      # opts :: options hash:
      #         :handler :: The name of a previously registered function used as a call handler for this language.
      #         :replace :: Replace the installed language if it already exists (on PostgreSQL 9.0+).
      #         :trusted :: Marks the language being created as trusted, allowing unprivileged users to create functions using this language.
      #         :validator :: The name of previously registered function used as a validator of functions defined in this language.
      def create_language(name, opts=OPTS)
        self << create_language_sql(name, opts)
      end

      # Create a schema in the database. Arguments:
      # name :: Name of the schema (e.g. admin)
      # opts :: options hash:
      #         :if_not_exists :: Don't raise an error if the schema already exists (PostgreSQL 9.3+)
      #         :owner :: The owner to set for the schema (defaults to current user if not specified)
      def create_schema(name, opts=OPTS)
        self << create_schema_sql(name, opts)
      end

      # Support partitions of tables using the :partition_of option.
      def create_table(name, options=OPTS, &block)
        if options[:partition_of]
          create_partition_of_table_from_generator(name, CreatePartitionOfTableGenerator.new(&block), options)
          return
        end

        super
      end

      # Support partitions of tables using the :partition_of option.
      def create_table?(name, options=OPTS, &block)
        if options[:partition_of]
          create_table(name, options.merge!(:if_not_exists=>true), &block)
          return
        end

        super
      end

      # Create a trigger in the database.  Arguments:
      # table :: the table on which this trigger operates
      # name :: the name of this trigger
      # function :: the function to call for this trigger, which should return type trigger.
      # opts :: options hash:
      #         :after :: Calls the trigger after execution instead of before.
      #         :args :: An argument or array of arguments to pass to the function.
      #         :each_row :: Calls the trigger for each row instead of for each statement.
      #         :events :: Can be :insert, :update, :delete, or an array of any of those. Calls the trigger whenever that type of statement is used.  By default,
      #                    the trigger is called for insert, update, or delete.
      #         :replace :: Replace the trigger with the same name if it already exists (PostgreSQL 14+).
      #         :when :: A filter to use for the trigger
      def create_trigger(table, name, function, opts=OPTS)
        self << create_trigger_sql(table, name, function, opts)
      end

      def database_type
        :postgres
      end

      # For constraints that are deferrable, defer constraints until 
      # transaction commit. Options:
      #
      # :constraints :: An identifier of the constraint, or an array of
      #                 identifiers for constraints, to apply this
      #                 change to specific constraints.
      # :server :: The server/shard on which to run the query.
      #
      # Examples:
      #
      #   DB.defer_constraints
      #   # SET CONSTRAINTS ALL DEFERRED
      #
      #   DB.defer_constraints(constraints: [:c1, Sequel[:sc][:c2]])
      #   # SET CONSTRAINTS "c1", "sc"."s2" DEFERRED
      def defer_constraints(opts=OPTS)
        _set_constraints(' DEFERRED', opts)
      end

      # Use PostgreSQL's DO syntax to execute an anonymous code block.  The code should
      # be the literal code string to use in the underlying procedural language.  Options:
      #
      # :language :: The procedural language the code is written in.  The PostgreSQL
      #              default is plpgsql.  Can be specified as a string or a symbol.
      def do(code, opts=OPTS)
        language = opts[:language]
        run "DO #{"LANGUAGE #{literal(language.to_s)} " if language}#{literal(code)}"
      end

      # Drops the function from the database. Arguments:
      # name :: name of the function to drop
      # opts :: options hash:
      #         :args :: The arguments for the function.  See create_function_sql.
      #         :cascade :: Drop other objects depending on this function.
      #         :if_exists :: Don't raise an error if the function doesn't exist.
      def drop_function(name, opts=OPTS)
        self << drop_function_sql(name, opts)
      end

      # Drops a procedural language from the database.  Arguments:
      # name :: name of the procedural language to drop
      # opts :: options hash:
      #         :cascade :: Drop other objects depending on this function.
      #         :if_exists :: Don't raise an error if the function doesn't exist.
      def drop_language(name, opts=OPTS)
        self << drop_language_sql(name, opts)
      end

      # Drops a schema from the database.  Arguments:
      # name :: name of the schema to drop
      # opts :: options hash:
      #         :cascade :: Drop all objects in this schema.
      #         :if_exists :: Don't raise an error if the schema doesn't exist.
      def drop_schema(name, opts=OPTS)
        self << drop_schema_sql(name, opts)
      end

      # Drops a trigger from the database.  Arguments:
      # table :: table from which to drop the trigger
      # name :: name of the trigger to drop
      # opts :: options hash:
      #         :cascade :: Drop other objects depending on this function.
      #         :if_exists :: Don't raise an error if the function doesn't exist.
      def drop_trigger(table, name, opts=OPTS)
        self << drop_trigger_sql(table, name, opts)
      end

      # Return full foreign key information using the pg system tables, including
      # :name, :on_delete, :on_update, and :deferrable entries in the hashes.
      #
      # Supports additional options:
      # :reverse :: Instead of returning foreign keys in the current table, return
      #             foreign keys in other tables that reference the current table.
      # :schema :: Set to true to have the :table value in the hashes be a qualified
      #            identifier.  Set to false to use a separate :schema value with
      #            the related schema.  Defaults to whether the given table argument
      #            is a qualified identifier.
      def foreign_key_list(table, opts=OPTS)
        m = output_identifier_meth
        schema, _ = opts.fetch(:schema, schema_and_table(table))

        h = {}
        fklod_map = FOREIGN_KEY_LIST_ON_DELETE_MAP 
        reverse = opts[:reverse]

        (reverse ? _reverse_foreign_key_list_ds : _foreign_key_list_ds).where_each(Sequel[:cl][:oid]=>regclass_oid(table)) do |row|
          if reverse
            key = [row[:schema], row[:table], row[:name]]
          else
            key = row[:name]
          end

          if r = h[key]
            r[:columns] << m.call(row[:column])
            r[:key] << m.call(row[:refcolumn])
          else
            entry = h[key] = {
              :name=>m.call(row[:name]),
              :columns=>[m.call(row[:column])],
              :key=>[m.call(row[:refcolumn])],
              :on_update=>fklod_map[row[:on_update]],
              :on_delete=>fklod_map[row[:on_delete]],
              :deferrable=>row[:deferrable],
              :validated=>row[:validated],
              :enforced=>row[:enforced],
              :table=>schema ? SQL::QualifiedIdentifier.new(m.call(row[:schema]), m.call(row[:table])) : m.call(row[:table]),
            }

            unless schema
              # If not combining schema information into the :table entry
              # include it as a separate entry.
              entry[:schema] = m.call(row[:schema])
            end
          end
        end

        h.values
      end

      def freeze
        server_version
        supports_prepared_transactions?
        _schema_ds
        _select_serial_sequence_ds
        _select_custom_sequence_ds
        _select_pk_ds
        _indexes_ds
        _check_constraints_ds
        _foreign_key_list_ds
        _reverse_foreign_key_list_ds
        @conversion_procs.freeze
        super
      end

      # Immediately apply deferrable constraints.
      #
      # :constraints :: An identifier of the constraint, or an array of
      #                 identifiers for constraints, to apply this
      #                 change to specific constraints.
      # :server :: The server/shard on which to run the query.
      #
      # Examples:
      #
      #   DB.immediate_constraints
      #   # SET CONSTRAINTS ALL IMMEDIATE
      #
      #   DB.immediate_constraints(constraints: [:c1, Sequel[:sc][:c2]])
      #   # SET CONSTRAINTS "c1", "sc"."s2" IMMEDIATE
      def immediate_constraints(opts=OPTS)
        _set_constraints(' IMMEDIATE', opts)
      end

      # Use the pg_* system tables to determine indexes on a table
      def indexes(table, opts=OPTS)
        m = output_identifier_meth
        cond = {Sequel[:tab][:oid]=>regclass_oid(table, opts)}
        cond[:indpred] = nil unless opts[:include_partial]

        indexes = {}
        _indexes_ds.where_each(cond) do |r|
          i = indexes[m.call(r[:name])] ||= {:columns=>[], :unique=>r[:unique], :deferrable=>r[:deferrable]}
          i[:columns] << m.call(r[:column])
        end
        indexes
      end

      # Dataset containing all current database locks
      def locks
        dataset.from(:pg_class).join(:pg_locks, :relation=>:relfilenode).select{[pg_class[:relname], Sequel::SQL::ColumnAll.new(:pg_locks)]}
      end

      # Notifies the given channel.  See the PostgreSQL NOTIFY documentation. Options:
      #
      # :payload :: The payload string to use for the NOTIFY statement.  Only supported
      #             in PostgreSQL 9.0+.
      # :server :: The server to which to send the NOTIFY statement, if the sharding support
      #            is being used.
      def notify(channel, opts=OPTS)
        sql = String.new
        sql << "NOTIFY "
        dataset.send(:identifier_append, sql, channel)
        if payload = opts[:payload]
          sql << ", "
          dataset.literal_append(sql, payload.to_s)
        end
        execute_ddl(sql, opts)
      end

      # Return primary key for the given table.
      def primary_key(table, opts=OPTS)
        quoted_table = quote_schema_table(table)
        Sequel.synchronize{return @primary_keys[quoted_table] if @primary_keys.has_key?(quoted_table)}
        value = _select_pk_ds.where_single_value(Sequel[:pg_class][:oid] => regclass_oid(table, opts))
        Sequel.synchronize{@primary_keys[quoted_table] = value}
      end

      # Return the sequence providing the default for the primary key for the given table.
      def primary_key_sequence(table, opts=OPTS)
        quoted_table = quote_schema_table(table)
        Sequel.synchronize{return @primary_key_sequences[quoted_table] if @primary_key_sequences.has_key?(quoted_table)}
        cond = {Sequel[:t][:oid] => regclass_oid(table, opts)}
        value = if pks = _select_serial_sequence_ds.first(cond)
          literal(SQL::QualifiedIdentifier.new(pks[:schema], pks[:sequence]))
        elsif pks = _select_custom_sequence_ds.first(cond)
          literal(SQL::QualifiedIdentifier.new(pks[:schema], LiteralString.new(pks[:sequence])))
        end

        Sequel.synchronize{@primary_key_sequences[quoted_table] = value} if value
      end

      # Refresh the materialized view with the given name.
      # 
      #   DB.refresh_view(:items_view)
      #   # REFRESH MATERIALIZED VIEW items_view
      #   DB.refresh_view(:items_view, concurrently: true)
      #   # REFRESH MATERIALIZED VIEW CONCURRENTLY items_view
      def refresh_view(name, opts=OPTS)
        run "REFRESH MATERIALIZED VIEW#{' CONCURRENTLY' if opts[:concurrently]} #{quote_schema_table(name)}"
      end
      
      # Reset the primary key sequence for the given table, basing it on the
      # maximum current value of the table's primary key.
      def reset_primary_key_sequence(table)
        return unless seq = primary_key_sequence(table)
        pk = SQL::Identifier.new(primary_key(table))
        db = self
        s, t = schema_and_table(table)
        table = Sequel.qualify(s, t) if s

        if server_version >= 100000
          seq_ds = metadata_dataset.from(:pg_sequence).where(:seqrelid=>regclass_oid(LiteralString.new(seq)))
          increment_by = :seqincrement
          min_value = :seqmin
        # :nocov:
        else
          seq_ds = metadata_dataset.from(LiteralString.new(seq))
          increment_by = :increment_by
          min_value = :min_value
        # :nocov:
        end

        get{setval(seq, db[table].select(coalesce(max(pk)+seq_ds.select(increment_by), seq_ds.select(min_value))), false)}
      end

      def rollback_prepared_transaction(transaction_id, opts=OPTS)
        run("ROLLBACK PREPARED #{literal(transaction_id)}", opts)
      end

      # PostgreSQL uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        # :nocov:
        auto_increment_key = server_version >= 100002 ? :identity : :serial
        # :nocov:
        {:primary_key => true, auto_increment_key => true, :type=>Integer}
      end

      # The version of the PostgreSQL server, used for determining capability.
      def server_version(server=nil)
        return @server_version if @server_version
        ds = dataset
        ds = ds.server(server) if server
        @server_version = swallow_database_error{ds.with_sql("SELECT CAST(current_setting('server_version_num') AS integer) AS v").single_value} || 0
      end

      # PostgreSQL supports CREATE TABLE IF NOT EXISTS on 9.1+
      def supports_create_table_if_not_exists?
        server_version >= 90100
      end

      # PostgreSQL 9.0+ supports some types of deferrable constraints beyond foreign key constraints.
      def supports_deferrable_constraints?
        server_version >= 90000
      end

      # PostgreSQL supports deferrable foreign key constraints.
      def supports_deferrable_foreign_key_constraints?
        true
      end

      # PostgreSQL supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
      end

      # PostgreSQL supports partial indexes.
      def supports_partial_indexes?
        true
      end

      # PostgreSQL 9.0+ supports trigger conditions.
      def supports_trigger_conditions?
        server_version >= 90000
      end

      # PostgreSQL supports prepared transactions (two-phase commit) if
      # max_prepared_transactions is greater than 0.
      def supports_prepared_transactions?
        return @supports_prepared_transactions if defined?(@supports_prepared_transactions)
        @supports_prepared_transactions = self['SHOW max_prepared_transactions'].get.to_i > 0
      end

      # PostgreSQL supports savepoints
      def supports_savepoints?
        true
      end

      # PostgreSQL supports transaction isolation levels
      def supports_transaction_isolation_levels?
        true
      end

      # PostgreSQL supports transaction DDL statements.
      def supports_transactional_ddl?
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
        pg_class_relname(['r', 'p'], opts, &block)
      end

      # Check whether the given type name string/symbol (e.g. :hstore) is supported by
      # the database.
      def type_supported?(type)
        Sequel.synchronize{return @supported_types[type] if @supported_types.has_key?(type)}
        supported = from(:pg_type).where(:typtype=>'b', :typname=>type.to_s).count > 0
        Sequel.synchronize{return @supported_types[type] = supported}
      end

      # Creates a dataset that uses the VALUES clause:
      #
      #   DB.values([[1, 2], [3, 4]])
      #   # VALUES ((1, 2), (3, 4))
      #
      #   DB.values([[1, 2], [3, 4]]).order(:column2).limit(1, 1)
      #   # VALUES ((1, 2), (3, 4)) ORDER BY column2 LIMIT 1 OFFSET 1
      def values(v)
        raise Error, "Cannot provide an empty array for values" if v.empty?
        @default_dataset.clone(:values=>v)
      end

      # Array of symbols specifying view names in the current database.
      #
      # Options:
      # :materialized :: Return materialized views
      # :qualify :: Return the views as Sequel::SQL::QualifiedIdentifier instances,
      #             using the schema the view is located in as the qualifier.
      # :schema :: The schema to search
      # :server :: The server to use
      def views(opts=OPTS)
        relkind = opts[:materialized] ? 'm' : 'v'
        pg_class_relname(relkind, opts)
      end

      # Attempt to acquire an exclusive advisory lock with the given lock_id (which should be
      # a 64-bit integer).  If successful, yield to the block, then release the advisory lock
      # when the block exits.  If unsuccessful, raise a Sequel::AdvisoryLockError.
      #
      #   DB.with_advisory_lock(1347){DB.get(1)}
      #   # SELECT pg_try_advisory_lock(1357) LIMIT 1
      #   # SELECT 1 AS v LIMIT 1
      #   # SELECT pg_advisory_unlock(1357) LIMIT 1
      #
      # Options:
      # :wait :: Do not raise an error, instead, wait until the advisory lock can be acquired.
      def with_advisory_lock(lock_id, opts=OPTS)
        ds = dataset
        if server = opts[:server]
          ds = ds.server(server)
        end
      
        synchronize(server) do |c|
          begin
            if opts[:wait]
              ds.get{pg_advisory_lock(lock_id)}
              locked = true
            else
              unless locked = ds.get{pg_try_advisory_lock(lock_id)}
                raise AdvisoryLockError, "unable to acquire advisory lock #{lock_id.inspect}"
              end
            end

            yield
          ensure
            ds.get{pg_advisory_unlock(lock_id)} if locked
          end
        end
      end

      private

      # Dataset used to retrieve CHECK constraint information
      def _check_constraints_ds
        @_check_constraints_ds ||= begin
          ds = metadata_dataset.
            from{pg_constraint.as(:co)}.
            left_join(Sequel[:pg_attribute].as(:att), :attrelid=>:conrelid, :attnum=>SQL::Function.new(:ANY, Sequel[:co][:conkey])).
            where(:contype=>'c').
            select{[co[:conname].as(:constraint), att[:attname].as(:column), pg_get_constraintdef(co[:oid]).as(:definition)]}

          _add_validated_enforced_constraint_columns(ds)
        end
      end

      # Dataset used to retrieve foreign keys referenced by a table
      def _foreign_key_list_ds
        @_foreign_key_list_ds ||= __foreign_key_list_ds(false)
      end

      # Dataset used to retrieve foreign keys referencing a table
      def _reverse_foreign_key_list_ds
        @_reverse_foreign_key_list_ds ||= __foreign_key_list_ds(true)
      end

      # Build dataset used for foreign key list methods.
      def __foreign_key_list_ds(reverse)
        if reverse
          ctable = Sequel[:att2]
          cclass = Sequel[:cl2]
          rtable = Sequel[:att]
          rclass = Sequel[:cl]
        else
          ctable = Sequel[:att]
          cclass = Sequel[:cl]
          rtable = Sequel[:att2]
          rclass = Sequel[:cl2]
        end

        if server_version >= 90500
          cpos = Sequel.expr{array_position(co[:conkey], ctable[:attnum])}
          rpos = Sequel.expr{array_position(co[:confkey], rtable[:attnum])}
        # :nocov:
        else
          range = 0...32
          cpos = Sequel.expr{SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(co[:conkey], [x]), x]}, 32, ctable[:attnum])}
          rpos = Sequel.expr{SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(co[:confkey], [x]), x]}, 32, rtable[:attnum])}
        # :nocov:
        end

        ds = metadata_dataset.
          from{pg_constraint.as(:co)}.
          join(Sequel[:pg_class].as(cclass), :oid=>:conrelid).
          join(Sequel[:pg_attribute].as(ctable), :attrelid=>:oid, :attnum=>SQL::Function.new(:ANY, Sequel[:co][:conkey])).
          join(Sequel[:pg_class].as(rclass), :oid=>Sequel[:co][:confrelid]).
          join(Sequel[:pg_attribute].as(rtable), :attrelid=>:oid, :attnum=>SQL::Function.new(:ANY, Sequel[:co][:confkey])).
          join(Sequel[:pg_namespace].as(:nsp), :oid=>Sequel[:cl2][:relnamespace]).
          order{[co[:conname], cpos]}.
          where{{
            cl[:relkind]=>%w'r p',
            co[:contype]=>'f',
            cpos=>rpos
          }}.
          select{[
            co[:conname].as(:name),
            ctable[:attname].as(:column),
            co[:confupdtype].as(:on_update),
            co[:confdeltype].as(:on_delete),
            cl2[:relname].as(:table),
            rtable[:attname].as(:refcolumn),
            SQL::BooleanExpression.new(:AND, co[:condeferrable], co[:condeferred]).as(:deferrable),
            nsp[:nspname].as(:schema)
          ]}

        if reverse
          ds = ds.order_append(Sequel[:nsp][:nspname], Sequel[:cl2][:relname])
        end

        _add_validated_enforced_constraint_columns(ds)
      end

      def _add_validated_enforced_constraint_columns(ds)
        validated_cond = if server_version >= 90100
          Sequel[:convalidated]
        # :nocov:
        else
          Sequel.cast(true, TrueClass)
        # :nocov:
        end
        ds = ds.select_append(validated_cond.as(:validated))

        enforced_cond = if server_version >= 180000
          Sequel[:conenforced]
        # :nocov:
        else
          Sequel.cast(true, TrueClass)
        # :nocov:
        end
        ds = ds.select_append(enforced_cond.as(:enforced))

        ds
      end

      # Dataset used to retrieve index information
      def _indexes_ds
        @_indexes_ds ||= begin
          if server_version >= 90500
            order = [Sequel[:indc][:relname], Sequel.function(:array_position, Sequel[:ind][:indkey], Sequel[:att][:attnum])]
          # :nocov:
          else
            range = 0...32
            order = [Sequel[:indc][:relname], SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(Sequel[:ind][:indkey], [x]), x]}, 32, Sequel[:att][:attnum])]
          # :nocov:
          end

          attnums = SQL::Function.new(:ANY, Sequel[:ind][:indkey])

          ds = metadata_dataset.
            from{pg_class.as(:tab)}.
            join(Sequel[:pg_index].as(:ind), :indrelid=>:oid).
            join(Sequel[:pg_class].as(:indc), :oid=>:indexrelid).
            join(Sequel[:pg_attribute].as(:att), :attrelid=>Sequel[:tab][:oid], :attnum=>attnums).
            left_join(Sequel[:pg_constraint].as(:con), :conname=>Sequel[:indc][:relname]).
            where{{
              indc[:relkind]=>%w'i I',
              ind[:indisprimary]=>false,
              :indexprs=>nil,
              :indisvalid=>true}}.
            order(*order).
            select{[indc[:relname].as(:name), ind[:indisunique].as(:unique), att[:attname].as(:column), con[:condeferrable].as(:deferrable)]}

          # :nocov:
          ds = ds.where(:indisready=>true) if server_version >= 80300
          ds = ds.where(:indislive=>true) if server_version >= 90300
          # :nocov:

          ds
        end
      end

      # Dataset used to determine custom serial sequences for tables
      def _select_custom_sequence_ds
        @_select_custom_sequence_ds ||= metadata_dataset.
          from{pg_class.as(:t)}.
          join(:pg_namespace, {:oid => :relnamespace}, :table_alias=>:name).
          join(:pg_attribute, {:attrelid => Sequel[:t][:oid]}, :table_alias=>:attr).
          join(:pg_attrdef, {:adrelid => :attrelid, :adnum => :attnum}, :table_alias=>:def).
          join(:pg_constraint, {:conrelid => :adrelid, Sequel[:cons][:conkey].sql_subscript(1) => :adnum}, :table_alias=>:cons).
          where{{cons[:contype] => 'p', pg_get_expr(self.def[:adbin], attr[:attrelid]) => /nextval/i}}.
          select{
            expr = split_part(pg_get_expr(self.def[:adbin], attr[:attrelid]), "'", 2)
            [
              name[:nspname].as(:schema),
              Sequel.case({{expr => /./} => substr(expr, strpos(expr, '.')+1)}, expr).as(:sequence)
            ]
          }
      end

      # Dataset used to determine normal serial sequences for tables
      def _select_serial_sequence_ds
        @_serial_sequence_ds ||= metadata_dataset.
          from{[
            pg_class.as(:seq),
            pg_attribute.as(:attr),
            pg_depend.as(:dep),
            pg_namespace.as(:name),
            pg_constraint.as(:cons),
            pg_class.as(:t)
          ]}.
          where{[
            [seq[:oid], dep[:objid]],
            [seq[:relnamespace], name[:oid]],
            [seq[:relkind], 'S'],
            [attr[:attrelid], dep[:refobjid]],
            [attr[:attnum], dep[:refobjsubid]],
            [attr[:attrelid], cons[:conrelid]],
            [attr[:attnum], cons[:conkey].sql_subscript(1)],
            [attr[:attrelid], t[:oid]],
            [cons[:contype], 'p']
          ]}.
          select{[
            name[:nspname].as(:schema),
            seq[:relname].as(:sequence)
          ]}
      end

      # Dataset used to determine primary keys for tables
      def _select_pk_ds
        @_select_pk_ds ||= metadata_dataset.
          from(:pg_class, :pg_attribute, :pg_index, :pg_namespace).
          where{[
            [pg_class[:oid], pg_attribute[:attrelid]],
            [pg_class[:relnamespace], pg_namespace[:oid]],
            [pg_class[:oid], pg_index[:indrelid]],
            [pg_index[:indkey].sql_subscript(0), pg_attribute[:attnum]],
            [pg_index[:indisprimary], 't']
          ]}.
          select{pg_attribute[:attname].as(:pk)}
      end

      # Dataset used to get schema for tables
      def _schema_ds
        @_schema_ds ||= begin
          ds = metadata_dataset.select{[
              pg_attribute[:attname].as(:name),
              SQL::Cast.new(pg_attribute[:atttypid], :integer).as(:oid),
              SQL::Cast.new(basetype[:oid], :integer).as(:base_oid),
              SQL::Function.new(:col_description, pg_class[:oid], pg_attribute[:attnum]).as(:comment),
              SQL::Function.new(:format_type, basetype[:oid], pg_type[:typtypmod]).as(:db_base_type),
              SQL::Function.new(:format_type, pg_type[:oid], pg_attribute[:atttypmod]).as(:db_type),
              SQL::Function.new(:pg_get_expr, pg_attrdef[:adbin], pg_class[:oid]).as(:default),
              SQL::BooleanExpression.new(:NOT, pg_attribute[:attnotnull]).as(:allow_null),
              SQL::Function.new(:COALESCE, SQL::BooleanExpression.from_value_pairs(pg_attribute[:attnum] => SQL::Function.new(:ANY, pg_index[:indkey])), false).as(:primary_key),
              Sequel[:pg_type][:typtype],
              (~Sequel[Sequel[:elementtype][:oid]=>nil]).as(:is_array),
            ]}.
            from(:pg_class).
            join(:pg_attribute, :attrelid=>:oid).
            join(:pg_type, :oid=>:atttypid).
            left_outer_join(Sequel[:pg_type].as(:basetype), :oid=>:typbasetype).
            left_outer_join(Sequel[:pg_type].as(:elementtype), :typarray=>Sequel[:pg_type][:oid]).
            left_outer_join(:pg_attrdef, :adrelid=>Sequel[:pg_class][:oid], :adnum=>Sequel[:pg_attribute][:attnum]).
            left_outer_join(:pg_index, :indrelid=>Sequel[:pg_class][:oid], :indisprimary=>true).
            where{{pg_attribute[:attisdropped]=>false}}.
            where{pg_attribute[:attnum] > 0}.
            order{pg_attribute[:attnum]}

          # :nocov:
          if server_version > 100000
          # :nocov:
            ds = ds.select_append{pg_attribute[:attidentity]}

            # :nocov:
            if server_version > 120000
            # :nocov:
              ds = ds.select_append{Sequel.~(pg_attribute[:attgenerated]=>'').as(:generated)}
            end
          end

          ds
        end
      end

      # Internals of defer_constraints/immediate_constraints
      def _set_constraints(type, opts)
        execute_ddl(_set_constraints_sql(type, opts), opts)
      end

      # SQL to use for SET CONSTRAINTS
      def _set_constraints_sql(type, opts)
        sql = String.new
        sql << "SET CONSTRAINTS "
        if constraints = opts[:constraints]
          dataset.send(:source_list_append, sql, Array(constraints))
        else
          sql << "ALL"
        end
        sql << type
      end

      # Consider lock or statement timeout errors as evidence that the table exists
      # but is locked.
      def _table_exists?(ds)
        super
      rescue DatabaseError => e    
        raise e unless /canceling statement due to (?:statement|lock) timeout/ =~ e.message 
      end
    
      def alter_table_add_column_sql(table, op)
        "ADD COLUMN#{' IF NOT EXISTS' if op[:if_not_exists]} #{column_definition_sql(op)}"
      end

      def alter_table_alter_constraint_sql(table, op)
        sql = String.new
        sql << "ALTER CONSTRAINT #{quote_identifier(op[:name])}"
        
        constraint_deferrable_sql_append(sql, op[:deferrable])

        case op[:enforced]
        when nil
        when false
          sql << " NOT ENFORCED"
        else
          sql << " ENFORCED"
        end

        case op[:inherit]
        when nil
        when false
          sql << " NO INHERIT"
        else
          sql << " INHERIT"
        end

        sql
      end

      def alter_table_generator_class
        Postgres::AlterTableGenerator
      end
    
      def alter_table_set_column_type_sql(table, op)
        s = super
        if using = op[:using]
          using = Sequel::LiteralString.new(using) if using.is_a?(String)
          s += ' USING '
          s << literal(using)
        end
        s
      end

      def alter_table_drop_column_sql(table, op)
        "DROP COLUMN #{'IF EXISTS ' if op[:if_exists]}#{quote_identifier(op[:name])}#{' CASCADE' if op[:cascade]}"
      end

      def alter_table_validate_constraint_sql(table, op)
        "VALIDATE CONSTRAINT #{quote_identifier(op[:name])}"
      end

      # If the :synchronous option is given and non-nil, set synchronous_commit
      # appropriately.  Valid values for the :synchronous option are true,
      # :on, false, :off, :local, and :remote_write.
      def begin_new_transaction(conn, opts)
        super
        if opts.has_key?(:synchronous)
          case sync = opts[:synchronous]
          when true
            sync = :on
          when false
            sync = :off
          when nil
            return
          end

          log_connection_execute(conn, "SET LOCAL synchronous_commit = #{sync}")
        end
      end
      
      # Set the READ ONLY transaction setting per savepoint, as PostgreSQL supports that.
      def begin_savepoint(conn, opts)
        super

        unless (read_only = opts[:read_only]).nil?
          log_connection_execute(conn, "SET TRANSACTION READ #{read_only ? 'ONLY' : 'WRITE'}")
        end
      end

      def column_definition_append_include_sql(sql, constraint)
        if include_cols = constraint[:include]
          sql << " INCLUDE " << literal(Array(include_cols))
        end
      end
    
      def column_definition_append_primary_key_sql(sql, constraint)
        super
        column_definition_append_include_sql(sql, constraint)
      end

      def column_definition_append_unique_sql(sql, constraint)
        super
        column_definition_append_include_sql(sql, constraint)
      end

      # Literalize non-String collate options. This is because unquoted collatations
      # are folded to lowercase, and PostgreSQL used mixed case or capitalized collations.
      def column_definition_collate_sql(sql, column)
        if collate = column[:collate]
          collate = literal(collate) unless collate.is_a?(String)
          sql << " COLLATE #{collate}"
        end
      end

      # Support identity columns, but only use the identity SQL syntax if no
      # default value is given.
      def column_definition_default_sql(sql, column)
        super
        if !column[:serial] && !['smallserial', 'serial', 'bigserial'].include?(column[:type].to_s) && !column[:default]
          if (identity = column[:identity])
            sql << " GENERATED "
            sql << (identity == :always ? "ALWAYS" : "BY DEFAULT")
            sql << " AS IDENTITY"
          elsif (generated = column[:generated_always_as])
            sql << " GENERATED ALWAYS AS (#{literal(generated)}) STORED"
          end
        end
      end

      # Handle PostgreSQL specific default format.
      def column_schema_normalize_default(default, type)
        if m = /\A(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))\z/.match(default)
          default = m[1] || m[2]
        end
        super(default, type)
      end

      # If the :prepare option is given and we aren't in a savepoint,
      # prepare the transaction for a two-phase commit.
      def commit_transaction(conn, opts=OPTS)
        if (s = opts[:prepare]) && savepoint_level(conn) <= 1
          log_connection_execute(conn, "PREPARE TRANSACTION #{literal(s)}")
        else
          super
        end
      end

      # PostgreSQL can't combine rename_column operations, and it can combine
      # validate_constraint and alter_constraint operations.
      def combinable_alter_table_op?(op)
        (super || op[:op] == :validate_constraint || op[:op] == :alter_constraint) && op[:op] != :rename_column
      end

      VALID_CLIENT_MIN_MESSAGES = %w'DEBUG5 DEBUG4 DEBUG3 DEBUG2 DEBUG1 LOG NOTICE WARNING ERROR FATAL PANIC'.freeze.each(&:freeze)
      # The SQL queries to execute when starting a new connection.
      def connection_configuration_sqls(opts=@opts)
        sqls = []

        sqls << "SET standard_conforming_strings = ON" if typecast_value_boolean(opts.fetch(:force_standard_strings, true))

        cmm = opts.fetch(:client_min_messages, :warning)
        if cmm && !cmm.to_s.empty?
          cmm = cmm.to_s.upcase.strip
          unless VALID_CLIENT_MIN_MESSAGES.include?(cmm)
            raise Error, "Unsupported client_min_messages setting: #{cmm}"
          end
          sqls << "SET client_min_messages = '#{cmm.to_s.upcase}'"
        end

        if search_path = opts[:search_path]
          case search_path
          when String
            search_path = search_path.split(",").map(&:strip)
          when Array
            # nil
          else
            raise Error, "unrecognized value for :search_path option: #{search_path.inspect}"
          end
          sqls << "SET search_path = #{search_path.map{|s| "\"#{s.gsub('"', '""')}\""}.join(',')}"
        end

        sqls
      end

      # Handle PostgreSQL-specific constraint features.
      def constraint_definition_sql(constraint)
        case type = constraint[:type]
        when :exclude
          elements = constraint[:elements].map{|c, op| "#{literal(c)} WITH #{op}"}.join(', ')
          sql = String.new
          sql << "CONSTRAINT #{quote_identifier(constraint[:name])} " if constraint[:name]
          sql << "EXCLUDE USING #{constraint[:using]||'gist'} (#{elements})"
          column_definition_append_include_sql(sql, constraint)
          sql << " WHERE #{filter_expr(constraint[:where])}" if constraint[:where]
          constraint_deferrable_sql_append(sql, constraint[:deferrable])
          sql
        when :primary_key, :unique
          sql = String.new
          sql << "CONSTRAINT #{quote_identifier(constraint[:name])} " if constraint[:name]

          if type == :primary_key
            sql << primary_key_constraint_sql_fragment(constraint)
          else
            sql << unique_constraint_sql_fragment(constraint)
          end

          if using_index = constraint[:using_index]
            sql << " USING INDEX " << quote_identifier(using_index)
          else
            cols = literal(constraint[:columns])
            cols.insert(-2, " WITHOUT OVERLAPS") if constraint[:without_overlaps]
            sql << " " << cols

            if include_cols = constraint[:include]
              sql << " INCLUDE " << literal(Array(include_cols))
            end
          end

          constraint_deferrable_sql_append(sql, constraint[:deferrable])
          sql
        else # when :foreign_key, :check
          sql = super
          if constraint[:no_inherit]
            sql << " NO INHERIT"
          end
          if constraint[:not_enforced]
            sql << " NOT ENFORCED"
          end
          if constraint[:not_valid]
            sql << " NOT VALID"
          end
          sql
        end
      end

      def column_definition_add_references_sql(sql, column)
        super
        if column[:not_enforced]
          sql << " NOT ENFORCED"
        end
      end

      def column_definition_null_sql(sql, column)
        constraint = column[:not_null]
        constraint = nil unless constraint.is_a?(Hash)
        if constraint && (name = constraint[:name])
          sql << " CONSTRAINT #{quote_identifier(name)}"
        end
        super
        if constraint && constraint[:no_inherit]
          sql << " NO INHERIT"
        end
      end

      # Handle :period option
      def column_references_table_constraint_sql(constraint)
        sql = String.new
        sql << "FOREIGN KEY "
        cols = constraint[:columns]
        cols = column_references_add_period(cols) if constraint[:period]
        sql << literal(cols) << column_references_sql(constraint)
      end

      def column_references_append_key_sql(sql, column)
        cols = Array(column[:key])
        cols = column_references_add_period(cols) if column[:period]
        sql << "(#{cols.map{|x| quote_identifier(x)}.join(', ')})"
      end

      def column_references_add_period(cols)
        cols= cols.dup
        cols[-1] = Sequel.lit("PERIOD #{quote_identifier(cols[-1])}")
        cols
      end
  
      def database_specific_error_class_from_sqlstate(sqlstate)
        if sqlstate == '23P01'
          ExclusionConstraintViolation
        elsif sqlstate == '40P01'
          SerializationFailure
        elsif sqlstate == '55P03'
          DatabaseLockTimeout
        else
          super
        end
      end

      DATABASE_ERROR_REGEXPS = [
        # Add this check first, since otherwise it's possible for users to control
        # which exception class is generated.
        [/invalid input syntax/, DatabaseError],
        [/duplicate key value violates unique constraint/, UniqueConstraintViolation],
        [/violates foreign key constraint/, ForeignKeyConstraintViolation],
        [/violates check constraint/, CheckConstraintViolation],
        [/violates not-null constraint/, NotNullConstraintViolation],
        [/conflicting key value violates exclusion constraint/, ExclusionConstraintViolation],
        [/could not serialize access/, SerializationFailure],
        [/could not obtain lock on row in relation/, DatabaseLockTimeout],
      ].freeze
      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # SQL for doing fast table insert from stdin.
      def copy_into_sql(table, opts)
        sql = String.new
        sql << "COPY #{literal(table)}"
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
        sql
      end

      # SQL for doing fast table output to stdout.
      def copy_table_sql(table, opts)
        if table.is_a?(String)
          table
        else
          if opts[:options] || opts[:format]
            options = String.new
            options << " ("
            options << "FORMAT #{opts[:format]}" if opts[:format]
            options << "#{', ' if opts[:format]}#{opts[:options]}" if opts[:options]
            options << ')'
          end
          table = if table.is_a?(::Sequel::Dataset)
            "(#{table.sql})"
          else
            literal(table)
          end
          "COPY #{table} TO STDOUT#{options}"
        end
      end

      # SQL statement to create database function.
      def create_function_sql(name, definition, opts=OPTS)
        args = opts[:args]
        in_out = %w'OUT INOUT'
        if (!opts[:args].is_a?(Array) || !opts[:args].any?{|a| Array(a).length == 3 && in_out.include?(a[2].to_s)})
          returns = opts[:returns] || 'void'
        end
        language = opts[:language] || 'SQL'
        <<-END
        CREATE#{' OR REPLACE' if opts[:replace]} FUNCTION #{name}#{sql_function_args(args)}
        #{"RETURNS #{returns}" if returns}
        LANGUAGE #{language}
        #{opts[:behavior].to_s.upcase if opts[:behavior]}
        #{'STRICT' if opts[:strict]}
        #{'SECURITY DEFINER' if opts[:security_definer]}
        #{"PARALLEL #{opts[:parallel].to_s.upcase}" if opts[:parallel]}
        #{"COST #{opts[:cost]}" if opts[:cost]}
        #{"ROWS #{opts[:rows]}" if opts[:rows]}
        #{opts[:set].map{|k,v| " SET #{k} = #{v}"}.join("\n") if opts[:set]}
        AS #{literal(definition.to_s)}#{", #{literal(opts[:link_symbol].to_s)}" if opts[:link_symbol]}
        END
      end

      # SQL for creating a procedural language.
      def create_language_sql(name, opts=OPTS)
        "CREATE#{' OR REPLACE' if opts[:replace] && server_version >= 90000}#{' TRUSTED' if opts[:trusted]} LANGUAGE #{name}#{" HANDLER #{opts[:handler]}" if opts[:handler]}#{" VALIDATOR #{opts[:validator]}" if opts[:validator]}"
      end

      # Create a partition of another table, used when the create_table with
      # the :partition_of option is given.
      def create_partition_of_table_from_generator(name, generator, options)
        execute_ddl(create_partition_of_table_sql(name, generator, options))
      end

      # SQL for creating a partition of another table.
      def create_partition_of_table_sql(name, generator, options)
        sql = create_table_prefix_sql(name, options).dup

        sql << " PARTITION OF #{quote_schema_table(options[:partition_of])}"

        case generator.partition_type
        when :range
          from, to = generator.range
          sql << " FOR VALUES FROM #{literal(from)} TO #{literal(to)}"
        when :list
          sql << " FOR VALUES IN #{literal(generator.list)}"
        when :hash
          mod, remainder = generator.hash_values
          sql << " FOR VALUES WITH (MODULUS #{literal(mod)}, REMAINDER #{literal(remainder)})"
        else # when :default
          sql << " DEFAULT"
        end

        sql << create_table_suffix_sql(name, options)

        sql
      end

      # SQL for creating a schema.
      def create_schema_sql(name, opts=OPTS)
        "CREATE SCHEMA #{'IF NOT EXISTS ' if opts[:if_not_exists]}#{quote_identifier(name)}#{" AUTHORIZATION #{literal(opts[:owner])}" if opts[:owner]}"
      end

      # DDL statement for creating a table with the given name, columns, and options
      def create_table_prefix_sql(name, options)
        prefix_sql = if options[:temp]
          raise(Error, "can't provide both :temp and :unlogged to create_table") if options[:unlogged]
          raise(Error, "can't provide both :temp and :foreign to create_table") if options[:foreign]
          temporary_table_sql
        elsif options[:foreign]
          raise(Error, "can't provide both :foreign and :unlogged to create_table") if options[:unlogged]
          'FOREIGN '
        elsif options.fetch(:unlogged){typecast_value_boolean(@opts[:unlogged_tables_default])}
          'UNLOGGED '
        end

        "CREATE #{prefix_sql}TABLE#{' IF NOT EXISTS' if options[:if_not_exists]} #{create_table_table_name_sql(name, options)}"
      end

      # SQL for creating a table with PostgreSQL specific options
      def create_table_sql(name, generator, options)
        "#{super}#{create_table_suffix_sql(name, options)}"
      end

      # Handle various PostgreSQl specific table extensions such as inheritance,
      # partitioning, tablespaces, and foreign tables.
      def create_table_suffix_sql(name, options)
        sql = String.new

        if inherits = options[:inherits]
          sql << " INHERITS (#{Array(inherits).map{|t| quote_schema_table(t)}.join(', ')})"
        end

        if partition_by = options[:partition_by]
          sql << " PARTITION BY #{options[:partition_type]||'RANGE'} #{literal(Array(partition_by))}"
        end

        if on_commit = options[:on_commit]
          raise(Error, "can't provide :on_commit without :temp to create_table") unless options[:temp]
          raise(Error, "unsupported on_commit option: #{on_commit.inspect}") unless ON_COMMIT.has_key?(on_commit)
          sql << " ON COMMIT #{ON_COMMIT[on_commit]}"
        end

        if tablespace = options[:tablespace]
          sql << " TABLESPACE #{quote_identifier(tablespace)}"
        end

        if server = options[:foreign]
          sql << " SERVER #{quote_identifier(server)}"
          if foreign_opts = options[:options]
            sql << " OPTIONS (#{foreign_opts.map{|k, v| "#{k} #{literal(v.to_s)}"}.join(', ')})"
          end
        end

        sql
      end

      def create_table_as_sql(name, sql, options)
        result = create_table_prefix_sql name, options
        if on_commit = options[:on_commit]
          result += " ON COMMIT #{ON_COMMIT[on_commit]}"
        end
        result += " AS #{sql}"
      end

      def create_table_generator_class
        Postgres::CreateTableGenerator
      end
    
      # SQL for creating a database trigger.
      def create_trigger_sql(table, name, function, opts=OPTS)
        events = opts[:events] ? Array(opts[:events]) : [:insert, :update, :delete]
        whence = opts[:after] ? 'AFTER' : 'BEFORE'
        if filter = opts[:when]
          raise Error, "Trigger conditions are not supported for this database" unless supports_trigger_conditions?
          filter = " WHEN #{filter_expr(filter)}"
        end
        "CREATE #{'OR REPLACE ' if opts[:replace]}TRIGGER #{name} #{whence} #{events.map{|e| e.to_s.upcase}.join(' OR ')} ON #{quote_schema_table(table)}#{' FOR EACH ROW' if opts[:each_row]}#{filter} EXECUTE PROCEDURE #{function}(#{Array(opts[:args]).map{|a| literal(a)}.join(', ')})"
      end

      # DDL fragment for initial part of CREATE VIEW statement
      def create_view_prefix_sql(name, options)
        sql = create_view_sql_append_columns("CREATE #{'OR REPLACE 'if options[:replace]}#{'TEMPORARY 'if options[:temp]}#{'RECURSIVE ' if options[:recursive]}#{'MATERIALIZED ' if options[:materialized]}VIEW #{quote_schema_table(name)}", options[:columns] || options[:recursive])

        if options[:security_invoker]
          sql += " WITH (security_invoker)"
        end

        if tablespace = options[:tablespace]
          sql += " TABLESPACE #{quote_identifier(tablespace)}"
        end

        sql
      end

      # SQL for dropping a function from the database.
      def drop_function_sql(name, opts=OPTS)
        "DROP FUNCTION#{' IF EXISTS' if opts[:if_exists]} #{name}#{sql_function_args(opts[:args])}#{' CASCADE' if opts[:cascade]}"
      end
      
      # Support :if_exists, :cascade, and :concurrently options.
      def drop_index_sql(table, op)
        sch, _ = schema_and_table(table)
        "DROP INDEX#{' CONCURRENTLY' if op[:concurrently]}#{' IF EXISTS' if op[:if_exists]} #{"#{quote_identifier(sch)}." if sch}#{quote_identifier(op[:name] || default_index_name(table, op[:columns]))}#{' CASCADE' if op[:cascade]}"
      end

      # SQL for dropping a procedural language from the database.
      def drop_language_sql(name, opts=OPTS)
        "DROP LANGUAGE#{' IF EXISTS' if opts[:if_exists]} #{name}#{' CASCADE' if opts[:cascade]}"
      end

      # SQL for dropping a schema from the database.
      def drop_schema_sql(name, opts=OPTS)
        "DROP SCHEMA#{' IF EXISTS' if opts[:if_exists]} #{quote_identifier(name)}#{' CASCADE' if opts[:cascade]}"
      end

      # SQL for dropping a trigger from the database.
      def drop_trigger_sql(table, name, opts=OPTS)
        "DROP TRIGGER#{' IF EXISTS' if opts[:if_exists]} #{name} ON #{quote_schema_table(table)}#{' CASCADE' if opts[:cascade]}"
      end

      # Support :foreign tables
      def drop_table_sql(name, options)
        "DROP#{' FOREIGN' if options[:foreign]} TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_schema_table(name)}#{' CASCADE' if options[:cascade]}"
      end

      # SQL for dropping a view from the database.
      def drop_view_sql(name, opts=OPTS)
        "DROP #{'MATERIALIZED ' if opts[:materialized]}VIEW#{' IF EXISTS' if opts[:if_exists]} #{quote_schema_table(name)}#{' CASCADE' if opts[:cascade]}"
      end

      # If opts includes a :schema option, use it, otherwise restrict the filter to only the
      # currently visible schemas.
      def filter_schema(ds, opts)
        expr = if schema = opts[:schema]
          if schema.is_a?(SQL::Identifier)
            schema.value.to_s
          else
            schema.to_s
          end
        else
          Sequel.function(:any, Sequel.function(:current_schemas, false))
        end
        ds.where{{pg_namespace[:nspname]=>expr}}
      end

      def index_definition_sql(table_name, index)
        cols = index[:columns]
        index_name = index[:name] || default_index_name(table_name, cols)

        expr = if o = index[:opclass]
          "(#{Array(cols).map{|c| "#{literal(c)} #{o}"}.join(', ')})"
        else
          literal(Array(cols))
        end

        if_not_exists = " IF NOT EXISTS" if index[:if_not_exists]
        unique = "UNIQUE " if index[:unique]
        index_type = index[:type]
        filter = index[:where] || index[:filter]
        filter = " WHERE #{filter_expr(filter)}" if filter
        nulls_distinct = " NULLS#{' NOT' if index[:nulls_distinct] == false} DISTINCT" unless index[:nulls_distinct].nil?

        case index_type
        when :full_text
          expr = "(to_tsvector(#{literal(index[:language] || 'simple')}::regconfig, #{literal(dataset.send(:full_text_string_join, cols))}))"
          index_type = index[:index_type] || :gin
        when :spatial
          index_type = :gist
        end

        "CREATE #{unique}INDEX#{' CONCURRENTLY' if index[:concurrently]}#{if_not_exists} #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{"USING #{index_type} " if index_type}#{expr}#{" INCLUDE #{literal(Array(index[:include]))}" if index[:include]}#{nulls_distinct}#{" TABLESPACE #{quote_identifier(index[:tablespace])}" if index[:tablespace]}#{filter}"
      end

      # Setup datastructures shared by all postgres adapters.
      def initialize_postgres_adapter
        @primary_keys = {}
        @primary_key_sequences = {}
        @supported_types = {}
        procs = @conversion_procs = CONVERSION_PROCS.dup
        procs[1184] = procs[1114] = method(:to_application_timestamp)
      end

      # Backbone of the tables and views support.
      def pg_class_relname(type, opts)
        ds = metadata_dataset.from(:pg_class).where(:relkind=>type).select(:relname).server(opts[:server]).join(:pg_namespace, :oid=>:relnamespace)
        ds = filter_schema(ds, opts)
        m = output_identifier_meth
        if defined?(yield)
          yield(ds)
        elsif opts[:qualify]
          ds.select_append{pg_namespace[:nspname]}.map{|r| Sequel.qualify(m.call(r[:nspname]).to_s, m.call(r[:relname]).to_s)}
        else
          ds.map{|r| m.call(r[:relname])}
        end
      end

      # Return an expression the oid for the table expr.  Used by the metadata parsing
      # code to disambiguate unqualified tables.
      def regclass_oid(expr, opts=OPTS)
        if expr.is_a?(String) && !expr.is_a?(LiteralString)
          expr = Sequel.identifier(expr)
        end

        sch, table = schema_and_table(expr)
        sch ||= opts[:schema]
        if sch
          expr = Sequel.qualify(sch, table)
        end
        
        expr = if ds = opts[:dataset]
          ds.literal(expr)
        else
          literal(expr)
        end

        Sequel.cast(expr.to_s,:regclass).cast(:oid)
      end

      # Remove the cached entries for primary keys and sequences when a table is changed.
      def remove_cached_schema(table)
        tab = quote_schema_table(table)
        Sequel.synchronize do
          @primary_keys.delete(tab)
          @primary_key_sequences.delete(tab)
        end
        super
      end

      # SQL DDL statement for renaming a table. PostgreSQL doesn't allow you to change a table's schema in
      # a rename table operation, so specifying a new schema in new_name will not have an effect.
      def rename_table_sql(name, new_name)
        "ALTER TABLE #{quote_schema_table(name)} RENAME TO #{quote_identifier(schema_and_table(new_name).last)}"
      end

      # Handle interval and citext types.
      def schema_column_type(db_type)
        case db_type
        when /\Ainterval\z/i
          :interval
        when /\Acitext\z/i
          :string
        else
          super
        end
      end

      # The schema :type entry to use for array types.
      def schema_array_type(db_type)
        :array
      end

      # The schema :type entry to use for row/composite types.
      def schema_composite_type(db_type)
        :composite
      end

      # The schema :type entry to use for enum types.
      def schema_enum_type(db_type)
        :enum
      end

      # The schema :type entry to use for range types.
      def schema_range_type(db_type)
        :range
      end

      # The schema :type entry to use for multirange types.
      def schema_multirange_type(db_type)
        :multirange
      end

      MIN_DATE = Date.new(-4713, 11, 24)
      MAX_DATE = Date.new(5874897, 12, 31)
      MIN_TIMESTAMP = Time.utc(-4713, 11, 24).freeze
      MAX_TIMESTAMP = (Time.utc(294277) - Rational(1, 1000000)).freeze
      TYPTYPE_METHOD_MAP = {
        'c' => :schema_composite_type,
        'e' => :schema_enum_type,
        'r' => :schema_range_type,
        'm' => :schema_multirange_type,
      }
      TYPTYPE_METHOD_MAP.default = :schema_column_type
      TYPTYPE_METHOD_MAP.freeze
      # The dataset used for parsing table schemas, using the pg_* system catalogs.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])

        _schema_ds.where_all(Sequel[:pg_class][:oid]=>regclass_oid(table_name, opts)).map do |row|
          row[:default] = nil if blank_object?(row[:default])
          if row[:base_oid]
            row[:domain_oid] = row[:oid]
            row[:oid] = row.delete(:base_oid)
            row[:db_domain_type] = row[:db_type]
            row[:db_type] = row.delete(:db_base_type)
          else
            row.delete(:base_oid)
            row.delete(:db_base_type)
          end

          db_type = row[:db_type]
          row[:type] = if row.delete(:is_array)
            schema_array_type(db_type)
          else
            send(TYPTYPE_METHOD_MAP[row.delete(:typtype)], db_type)
          end
          identity = row.delete(:attidentity)
          if row[:primary_key]
            row[:auto_increment] = !!(row[:default] =~ /\A(?:nextval)/i) || identity == 'a' || identity == 'd'
          end

          # :nocov:
          if server_version >= 90600
          # :nocov:
            case row[:oid]
            when 1082
              row[:min_value] = MIN_DATE
              row[:max_value] = MAX_DATE
            when 1184, 1114
              if Sequel.datetime_class == Time
                row[:min_value] = MIN_TIMESTAMP
                row[:max_value] = MAX_TIMESTAMP
              end
            end
          end

          [m.call(row.delete(:name)), row]
        end
      end

      # Set the transaction isolation level on the given connection
      def set_transaction_isolation(conn, opts)
        level = opts.fetch(:isolation, transaction_isolation_level)
        read_only = opts[:read_only]
        deferrable = opts[:deferrable]
        if level || !read_only.nil? || !deferrable.nil?
          sql = String.new
          sql << "SET TRANSACTION"
          sql << " ISOLATION LEVEL #{Sequel::Database::TRANSACTION_ISOLATION_LEVELS[level]}" if level
          sql << " READ #{read_only ? 'ONLY' : 'WRITE'}" unless read_only.nil?
          sql << " #{'NOT ' unless deferrable}DEFERRABLE" unless deferrable.nil?
          log_connection_execute(conn, sql)
        end
      end
     
      # Turns an array of argument specifiers into an SQL fragment used for function arguments.  See create_function_sql.
      def sql_function_args(args)
        "(#{Array(args).map{|a| Array(a).reverse.join(' ')}.join(', ')})"
      end

      # PostgreSQL can combine multiple alter table ops into a single query.
      def supports_combining_alter_table_ops?
        true
      end

      # PostgreSQL supports CREATE OR REPLACE VIEW.
      def supports_create_or_replace_view?
        true
      end

      # Handle bigserial type if :serial option is present
      def type_literal_generic_bignum_symbol(column)
        column[:serial] ? :bigserial : super
      end

      # PostgreSQL uses the bytea data type for blobs
      def type_literal_generic_file(column)
        :bytea
      end

      # Handle serial type if :serial option is present
      def type_literal_generic_integer(column)
        column[:serial] ? :serial : super
      end

      # PostgreSQL prefers the text datatype.  If a fixed size is requested,
      # the char type is used.  If the text type is specifically
      # disallowed or there is a size specified, use the varchar type.
      # Otherwise use the text type.
      def type_literal_generic_string(column)
        if column[:text]
          :text
        elsif column[:fixed]
          "char(#{column[:size]||default_string_column_size})"
        elsif column[:text] == false || column[:size]
          "varchar(#{column[:size]||default_string_column_size})"
        else
          :text
        end
      end

      # Support :nulls_not_distinct option.
      def unique_constraint_sql_fragment(constraint)
        if constraint[:nulls_not_distinct]
          'UNIQUE NULLS NOT DISTINCT'
        else
          'UNIQUE'
        end
      end
    
      # PostgreSQL 9.4+ supports views with check option.
      def view_with_check_option_support
        # :nocov:
        :local if server_version >= 90400
        # :nocov:
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      NULL = LiteralString.new('NULL').freeze
      LOCK_MODES = ['ACCESS SHARE', 'ROW SHARE', 'ROW EXCLUSIVE', 'SHARE UPDATE EXCLUSIVE', 'SHARE', 'SHARE ROW EXCLUSIVE', 'EXCLUSIVE', 'ACCESS EXCLUSIVE'].each(&:freeze).freeze

      Dataset.def_sql_method(self, :delete, [['if server_version >= 90100', %w'with delete from using where returning'], ['else', %w'delete from using where returning']])
      Dataset.def_sql_method(self, :insert, [['if server_version >= 90500', %w'with insert into columns override values conflict returning'], ['elsif server_version >= 90100', %w'with insert into columns values returning'], ['else', %w'insert into columns values returning']])
      Dataset.def_sql_method(self, :select, [['if opts[:values]', %w'values compounds order limit'], ['elsif server_version >= 80400', %w'with select distinct columns from join where group having window compounds order limit lock'], ['else', %w'select distinct columns from join where group having compounds order limit lock']])
      Dataset.def_sql_method(self, :update, [['if server_version >= 90100', %w'with update table set from where returning'], ['else', %w'update table set from where returning']])

      # Return the results of an EXPLAIN ANALYZE query as a string
      def analyze
        explain(:analyze=>true)
      end

      # Handle converting the ruby xor operator (^) into the
      # PostgreSQL xor operator (#), and use the ILIKE and NOT ILIKE
      # operators.
      def complex_expression_sql_append(sql, op, args)
        case op
        when :^
          j = ' # '
          c = false
          args.each do |a|
            sql << j if c
            literal_append(sql, a)
            c ||= true
          end
        when :ILIKE, :'NOT ILIKE'
          sql << '('
          literal_append(sql, args[0])
          sql << ' ' << op.to_s << ' '
          literal_append(sql, args[1])
          sql << ')'
        else
          super
        end
      end

      # Disables automatic use of INSERT ... RETURNING.  You can still use
      # returning manually to force the use of RETURNING when inserting.
      #
      # This is designed for cases where INSERT RETURNING cannot be used,
      # such as when you are using partitioning with trigger functions
      # or conditional rules, or when you are using a PostgreSQL version
      # less than 8.2, or a PostgreSQL derivative that does not support
      # returning.
      #
      # Note that when this method is used, insert will not return the
      # primary key of the inserted row, you will have to get the primary
      # key of the inserted row before inserting via nextval, or after
      # inserting via currval or lastval (making sure to use the same
      # database connection for currval or lastval).
      def disable_insert_returning
        clone(:disable_insert_returning=>true)
      end

      # Always return false when using VALUES
      def empty?
        return false if @opts[:values]
        super
      end

      # Return the results of an EXPLAIN query.  Boolean options:
      #
      # :analyze :: Use the ANALYZE option.
      # :buffers :: Use the BUFFERS option.
      # :costs :: Use the COSTS option.
      # :generic_plan :: Use the GENERIC_PLAN option.
      # :memory :: Use the MEMORY option.
      # :settings :: Use the SETTINGS option.
      # :summary :: Use the SUMMARY option.
      # :timing :: Use the TIMING option.
      # :verbose :: Use the VERBOSE option.
      # :wal :: Use the WAL option.
      #
      # Non boolean options:
      #
      # :format :: Use the FORMAT option to change the format of the
      #            returned value.  Values can be :text, :xml, :json,
      #            or :yaml.
      # :serialize :: Use the SERIALIZE option to get timing on
      #               serialization.  Values can be :none, :text, or
      #               :binary.
      #
      # See the PostgreSQL EXPLAIN documentation for an explanation of
      # what each option does.
      #
      # In most cases, the return value is a single string.  However,
      # using the <tt>format: :json</tt> option can result in the return
      # value being an array containing a hash.
      def explain(opts=OPTS)
        rows = clone(:append_sql=>explain_sql_string_origin(opts)).map(:'QUERY PLAN')

        if rows.length == 1
          rows[0]
        elsif rows.all?{|row| String === row}
          rows.join("\r\n") 
        # :nocov:
        else
          # This branch is unreachable in tests, but it seems better to just return
          # all rows than throw in error if this case actually happens.
          rows
        # :nocov:
        end
      end

      # Return a cloned dataset which will use FOR KEY SHARE to lock returned rows.
      # Supported on PostgreSQL 9.3+.
      def for_key_share
        cached_lock_style_dataset(:_for_key_share_ds, :key_share)
      end

      # Return a cloned dataset which will use FOR NO KEY UPDATE to lock returned rows.
      # This is generally a better choice than using for_update on PostgreSQL, unless
      # you will be deleting the row or modifying a key column. Supported on PostgreSQL 9.3+.
      def for_no_key_update
        cached_lock_style_dataset(:_for_no_key_update_ds, :no_key_update)
      end

      # Return a cloned dataset which will use FOR SHARE to lock returned rows.
      def for_share
        cached_lock_style_dataset(:_for_share_ds, :share)
      end

      # Run a full text search on PostgreSQL.  By default, searching for the inclusion
      # of any of the terms in any of the cols.
      #
      # Options:
      # :headline :: Append a expression to the selected columns aliased to headline that
      #              contains an extract of the matched text.
      # :language :: The language to use for the search (default: 'simple')
      # :plain :: Whether a plain search should be used (default: false).  In this case,
      #           terms should be a single string, and it will do a search where cols
      #           contains all of the words in terms.  This ignores search operators in terms.
      # :phrase :: Similar to :plain, but also adding an ILIKE filter to ensure that
      #            returned rows also include the exact phrase used.
      # :rank :: Set to true to order by the rank, so that closer matches are returned first.
      # :to_tsquery :: Can be set to :plain, :phrase, or :websearch to specify the function to use to
      #                convert the terms to a ts_query.
      # :tsquery :: Specifies the terms argument is already a valid SQL expression returning a
      #             tsquery, and can be used directly in the query.
      # :tsvector :: Specifies the cols argument is already a valid SQL expression returning a
      #              tsvector, and can be used directly in the query.
      def full_text_search(cols, terms, opts = OPTS)
        lang = Sequel.cast(opts[:language] || 'simple', :regconfig)

        unless opts[:tsvector]
          phrase_cols = full_text_string_join(cols)
          cols = Sequel.function(:to_tsvector, lang, phrase_cols)
        end

        unless opts[:tsquery]
          phrase_terms = terms.is_a?(Array) ? terms.join(' | ') : terms

          query_func = case to_tsquery = opts[:to_tsquery]
          when :phrase, :plain
            :"#{to_tsquery}to_tsquery"
          when :websearch
            :"websearch_to_tsquery"
          else
            (opts[:phrase] || opts[:plain]) ? :plainto_tsquery : :to_tsquery
          end

          terms = Sequel.function(query_func, lang, phrase_terms)
        end

        ds = where(Sequel.lit(["", " @@ ", ""], cols, terms))

        if opts[:phrase]
          raise Error, "can't use :phrase with either :tsvector or :tsquery arguments to full_text_search together" if opts[:tsvector] || opts[:tsquery]
          ds = ds.grep(phrase_cols, "%#{escape_like(phrase_terms)}%", :case_insensitive=>true)
        end

        if opts[:rank]
          ds = ds.reverse{ts_rank_cd(cols, terms)}
        end

        if opts[:headline]
          ds = ds.select_append{ts_headline(lang, phrase_cols, terms).as(:headline)}
        end

        ds
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

      # Handle uniqueness violations when inserting, by updating the conflicting row, using
      # ON CONFLICT. With no options, uses ON CONFLICT DO NOTHING.  Options:
      # :conflict_where :: The index filter, when using a partial index to determine uniqueness.
      # :constraint :: An explicit constraint name, has precendence over :target.
      # :target :: The column name or expression to handle uniqueness violations on.
      # :update :: A hash of columns and values to set.  Uses ON CONFLICT DO UPDATE.
      # :update_where :: A WHERE condition to use for the update.
      #
      # Examples:
      #
      #   DB[:table].insert_conflict.insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT DO NOTHING
      #   
      #   DB[:table].insert_conflict(constraint: :table_a_uidx).insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT ON CONSTRAINT table_a_uidx DO NOTHING
      #   
      #   DB[:table].insert_conflict(target: :a).insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT (a) DO NOTHING
      #
      #   DB[:table].insert_conflict(target: :a, conflict_where: {c: true}).insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT (a) WHERE (c IS TRUE) DO NOTHING
      #   
      #   DB[:table].insert_conflict(target: :a, update: {b: Sequel[:excluded][:b]}).insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT (a) DO UPDATE SET b = excluded.b
      #   
      #   DB[:table].insert_conflict(constraint: :table_a_uidx,
      #     update: {b: Sequel[:excluded][:b]}, update_where: {Sequel[:table][:status_id] => 1}).insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT ON CONSTRAINT table_a_uidx
      #   # DO UPDATE SET b = excluded.b WHERE (table.status_id = 1)
      def insert_conflict(opts=OPTS)
        clone(:insert_conflict => opts)
      end

      # Ignore uniqueness/exclusion violations when inserting, using ON CONFLICT DO NOTHING.
      # Exists mostly for compatibility to MySQL's insert_ignore. Example:
      #
      #   DB[:table].insert_ignore.insert(a: 1, b: 2)
      #   # INSERT INTO TABLE (a, b) VALUES (1, 2)
      #   # ON CONFLICT DO NOTHING
      def insert_ignore
        insert_conflict
      end

      # Insert a record, returning the record inserted, using RETURNING.  Always returns nil without
      # running an INSERT statement if disable_insert_returning is used.  If the query runs
      # but returns no values, returns false.
      def insert_select(*values)
        return unless supports_insert_select?
        # Handle case where query does not return a row
        server?(:default).with_sql_first(insert_select_sql(*values)) || false
      end

      # The SQL to use for an insert_select, adds a RETURNING clause to the insert
      # unless the RETURNING clause is already present.
      def insert_select_sql(*values)
        ds = opts[:returning] ? self : returning
        ds.insert_sql(*values)
      end

      # Support SQL::AliasedExpression as expr to setup a USING join with a table alias for the
      # USING columns.
      def join_table(type, table, expr=nil, options=OPTS, &block)
        if expr.is_a?(SQL::AliasedExpression) && expr.expression.is_a?(Array) && !expr.expression.empty? && expr.expression.all?
          options = options.merge(:join_using=>true)
        end
        super
      end

      # Locks all tables in the dataset's FROM clause (but not in JOINs) with
      # the specified mode (e.g. 'EXCLUSIVE').  If a block is given, starts
      # a new transaction, locks the table, and yields.  If a block is not given,
      # just locks the tables.  Note that PostgreSQL will probably raise an error
      # if you lock the table outside of an existing transaction.  Returns nil.
      def lock(mode, opts=OPTS)
        if defined?(yield) # perform locking inside a transaction and yield to block
          @db.transaction(opts){lock(mode, opts); yield}
        else
          sql = 'LOCK TABLE '.dup
          source_list_append(sql, @opts[:from])
          mode = mode.to_s.upcase.strip
          unless LOCK_MODES.include?(mode)
            raise Error, "Unsupported lock mode: #{mode}"
          end
          sql << " IN #{mode} MODE"
          @db.execute(sql, opts)
        end
        nil
      end

      # Support MERGE RETURNING on PostgreSQL 17+.
      def merge(&block)
        sql = merge_sql
        if uses_returning?(:merge)
          returning_fetch_rows(sql, &block)
        else
          execute_ddl(sql)
        end
      end

      # Return a dataset with a WHEN NOT MATCHED BY SOURCE THEN DELETE clause added to the
      # MERGE statement.  If a block is passed, treat it as a virtual row and
      # use it as additional conditions for the match.
      #
      #   merge_delete_not_matched_by_source
      #   # WHEN NOT MATCHED BY SOURCE THEN DELETE
      #
      #   merge_delete_not_matched_by_source{a > 30}
      #   # WHEN NOT MATCHED BY SOURCE AND (a > 30) THEN DELETE
      def merge_delete_when_not_matched_by_source(&block)
        _merge_when(:type=>:delete_not_matched_by_source, &block)
      end

      # Return a dataset with a WHEN MATCHED THEN DO NOTHING clause added to the
      # MERGE statement.  If a block is passed, treat it as a virtual row and
      # use it as additional conditions for the match.
      #
      #   merge_do_nothing_when_matched
      #   # WHEN MATCHED THEN DO NOTHING
      #
      #   merge_do_nothing_when_matched{a > 30}
      #   # WHEN MATCHED AND (a > 30) THEN DO NOTHING
      def merge_do_nothing_when_matched(&block)
        _merge_when(:type=>:matched, &block)
      end

      # Return a dataset with a WHEN NOT MATCHED THEN DO NOTHING clause added to the
      # MERGE statement.  If a block is passed, treat it as a virtual row and
      # use it as additional conditions for the match.
      #
      #   merge_do_nothing_when_not_matched
      #   # WHEN NOT MATCHED THEN DO NOTHING
      #
      #   merge_do_nothing_when_not_matched{a > 30}
      #   # WHEN NOT MATCHED AND (a > 30) THEN DO NOTHING
      def merge_do_nothing_when_not_matched(&block)
        _merge_when(:type=>:not_matched, &block)
      end

      # Return a dataset with a WHEN NOT MATCHED BY SOURCE THEN DO NOTHING clause added to the
      # MERGE BY SOURCE statement.  If a block is passed, treat it as a virtual row and
      # use it as additional conditions for the match.
      #
      #   merge_do_nothing_when_not_matched_by_source
      #   # WHEN NOT MATCHED BY SOURCE THEN DO NOTHING
      #
      #   merge_do_nothing_when_not_matched_by_source{a > 30}
      #   # WHEN NOT MATCHED BY SOURCE AND (a > 30) THEN DO NOTHING
      def merge_do_nothing_when_not_matched_by_source(&block)
        _merge_when(:type=>:not_matched_by_source, &block)
      end

      # Support OVERRIDING USER|SYSTEM VALUE for MERGE INSERT.
      def merge_insert(*values, &block)
        h = {:type=>:insert, :values=>values}
        if @opts[:override]
          h[:override] = insert_override_sql(String.new)
        end
        _merge_when(h, &block)
      end
    
      # Return a dataset with a WHEN NOT MATCHED BY SOURCE THEN UPDATE clause added to the
      # MERGE statement.  If a block is passed, treat it as a virtual row and
      # use it as additional conditions for the match.
      #
      #   merge_update_not_matched_by_source(i1: Sequel[:i1]+:i2+10, a: Sequel[:a]+:b+20)
      #   # WHEN NOT MATCHED BY SOURCE THEN UPDATE SET i1 = (i1 + i2 + 10), a = (a + b + 20)
      #
      #   merge_update_not_matched_by_source(i1: :i2){a > 30}
      #   # WHEN NOT MATCHED BY SOURCE AND (a > 30) THEN UPDATE SET i1 = i2
      def merge_update_when_not_matched_by_source(values, &block)
        _merge_when(:type=>:update_not_matched_by_source, :values=>values, &block)
      end

      # Use OVERRIDING USER VALUE for INSERT statements, so that identity columns
      # always use the user supplied value, and an error is not raised for identity
      # columns that are GENERATED ALWAYS.
      def overriding_system_value
        clone(:override=>:system)
      end

      # Use OVERRIDING USER VALUE for INSERT statements, so that identity columns
      # always use the sequence value instead of the user supplied value.
      def overriding_user_value
        clone(:override=>:user)
      end

      def supports_cte?(type=:select)
        if type == :select
          server_version >= 80400
        else
          server_version >= 90100
        end
      end

      # PostgreSQL supports using the WITH clause in subqueries if it
      # supports using WITH at all (i.e. on PostgreSQL 8.4+).
      def supports_cte_in_subqueries?
        supports_cte?
      end

      # DISTINCT ON is a PostgreSQL extension
      def supports_distinct_on?
        true
      end

      # PostgreSQL 9.5+ supports GROUP CUBE
      def supports_group_cube?
        server_version >= 90500
      end

      # PostgreSQL 9.5+ supports GROUP ROLLUP
      def supports_group_rollup?
        server_version >= 90500
      end

      # PostgreSQL 9.5+ supports GROUPING SETS
      def supports_grouping_sets?
        server_version >= 90500
      end

      # True unless insert returning has been disabled for this dataset.
      def supports_insert_select?
        !@opts[:disable_insert_returning]
      end

      # PostgreSQL 9.5+ supports the ON CONFLICT clause to INSERT.
      def supports_insert_conflict?
        server_version >= 90500
      end

      # PostgreSQL 9.3+ supports lateral subqueries
      def supports_lateral_subqueries?
        server_version >= 90300
      end
      
      # PostgreSQL supports modifying joined datasets
      def supports_modifying_joins?
        true
      end

      # PostgreSQL 15+ supports MERGE.
      def supports_merge?
        server_version >= 150000
      end

      # PostgreSQL supports NOWAIT.
      def supports_nowait?
        true
      end

      # MERGE RETURNING is supported on PostgreSQL 17+. Other RETURNING is supported
      # on all supported PostgreSQL versions.
      def supports_returning?(type)
        if type == :merge
          server_version >= 170000
        else
          true
        end
      end

      # PostgreSQL supports pattern matching via regular expressions
      def supports_regexp?
        true
      end

      # PostgreSQL 9.5+ supports SKIP LOCKED.
      def supports_skip_locked?
        server_version >= 90500
      end

      # :nocov:

      # PostgreSQL supports timezones in literal timestamps
      def supports_timestamp_timezones?
        # SEQUEL6: Remove
        true
      end
      # :nocov:

      # PostgreSQL 8.4+ supports WINDOW clause.
      def supports_window_clause?
        server_version >= 80400
      end

      # PostgreSQL 8.4+ supports window functions
      def supports_window_functions?
        server_version >= 80400
      end

      # Base support added in 8.4, offset supported added in 9.0,
      # GROUPS and EXCLUDE support added in 11.0.
      def supports_window_function_frame_option?(option)
        case option
        when :rows, :range
          true
        when :offset
          server_version >= 90000
        when :groups, :exclude
          server_version >= 110000
        else
          false
        end
      end
    
      # Truncates the dataset.  Returns nil.
      #
      # Options:
      # :cascade :: whether to use the CASCADE option, useful when truncating
      #             tables with foreign keys.
      # :only :: truncate using ONLY, so child tables are unaffected
      # :restart :: use RESTART IDENTITY to restart any related sequences
      #
      # :only and :restart only work correctly on PostgreSQL 8.4+.
      #
      # Usage:
      #   DB[:table].truncate
      #   # TRUNCATE TABLE "table"
      #
      #   DB[:table].truncate(cascade: true, only: true, restart: true)
      #   # TRUNCATE TABLE ONLY "table" RESTART IDENTITY CASCADE
      def truncate(opts = OPTS)
        if opts.empty?
          super()
        else
          clone(:truncate_opts=>opts).truncate
        end
      end

      # Use WITH TIES when limiting the result set to also include additional
      # rules that have the same results for the order column as the final row.
      # Requires PostgreSQL 13.
      def with_ties
        clone(:limit_with_ties=>true)
      end

      protected

      # If returned primary keys are requested, use RETURNING unless already set on the
      # dataset.  If RETURNING is already set, use existing returning values.  If RETURNING
      # is only set to return a single columns, return an array of just that column.
      # Otherwise, return an array of hashes.
      def _import(columns, values, opts=OPTS)
        if @opts[:returning]
          # no transaction: our multi_insert_sql_strategy should guarantee
          # that there's only ever a single statement.
          sql = multi_insert_sql(columns, values)[0]
          returning_fetch_rows(sql).map{|v| v.length == 1 ? v.values.first : v}
        elsif opts[:return] == :primary_key
          returning(insert_pk)._import(columns, values, opts)
        else
          super
        end
      end

      def to_prepared_statement(type, *a)
        if type == :insert && !@opts.has_key?(:returning)
          returning(insert_pk).send(:to_prepared_statement, :insert_pk, *a)
        else
          super
        end
      end

      private

      # Append the INSERT sql used in a MERGE
      def _merge_insert_sql(sql, data)
        sql << " THEN INSERT"
        columns, values = _parse_insert_sql_args(data[:values])
        _insert_columns_sql(sql, columns)
        if override = data[:override]
          sql << override
        end
        _insert_values_sql(sql, values)
      end

      def _merge_do_nothing_sql(sql, data)
        sql << " THEN DO NOTHING"
      end

      # Support MERGE RETURNING on PostgreSQL 17+.
      def _merge_when_sql(sql)
        super
        insert_returning_sql(sql) if uses_returning?(:merge)
      end

      # Format TRUNCATE statement with PostgreSQL specific options.
      def _truncate_sql(table)
        to = @opts[:truncate_opts] || OPTS
        "TRUNCATE TABLE#{' ONLY' if to[:only]} #{table}#{' RESTART IDENTITY' if to[:restart]}#{' CASCADE' if to[:cascade]}"
      end

      # Use from_self for aggregate dataset using VALUES.
      def aggreate_dataset_use_from_self?
        super || @opts[:values]
      end
      
      # Allow truncation of multiple source tables.
      def check_truncation_allowed!
        raise(InvalidOperation, "Grouped datasets cannot be truncated") if opts[:group]
        raise(InvalidOperation, "Joined datasets cannot be truncated") if opts[:join]
      end

      # The strftime format to use when literalizing the time.
      def default_timestamp_format
        "'%Y-%m-%d %H:%M:%S.%6N%z'"
      end

      # Only include the primary table in the main delete clause
      def delete_from_sql(sql)
        sql << ' FROM '
        source_list_append(sql, @opts[:from][0..0])
      end

      # Use USING to specify additional tables in a delete query
      def delete_using_sql(sql)
        join_from_sql(:USING, sql)
      end

      # Handle column aliases containing data types, useful for selecting from functions
      # that return the record data type.
      def derived_column_list_sql_append(sql, column_aliases)
        c = false
        comma = ', '
        column_aliases.each do |a|
          sql << comma if c
          if a.is_a?(Array)
            raise Error, "column aliases specified as arrays must have only 2 elements, the first is alias name and the second is data type" unless a.length == 2
            a, type = a
            identifier_append(sql, a)
            sql << " " << db.cast_type_literal(type).to_s
          else
            identifier_append(sql, a)
          end
          c ||= true
        end
      end

      EXPLAIN_BOOLEAN_OPTIONS = {}
      %w[analyze verbose costs settings generic_plan buffers wal timing summary memory].each do |str|
        EXPLAIN_BOOLEAN_OPTIONS[str.to_sym] = str.upcase.freeze
      end
      EXPLAIN_BOOLEAN_OPTIONS.freeze

      EXPLAIN_NONBOOLEAN_OPTIONS = {
        :serialize => {:none=>"SERIALIZE NONE", :text=>"SERIALIZE TEXT", :binary=>"SERIALIZE BINARY"}.freeze,
        :format => {:text=>"FORMAT TEXT", :xml=>"FORMAT XML", :json=>"FORMAT JSON", :yaml=>"FORMAT YAML"}.freeze
      }.freeze
    
      # A mutable string used as the prefix when explaining a query.
      def explain_sql_string_origin(opts)
        origin = String.new
        origin << 'EXPLAIN '

        # :nocov:
        if server_version < 90000
          if opts[:analyze]
            origin << 'ANALYZE '
          end

          return origin
        end
        # :nocov:

        comma = nil
        paren = "("

        add_opt = lambda do |str, value|
          origin << paren if paren
          origin << comma if comma
          origin << str
          origin << " FALSE" unless value
          comma ||= ', '
          paren &&= nil
        end

        EXPLAIN_BOOLEAN_OPTIONS.each do |key, str|
          unless (value = opts[key]).nil?
            add_opt.call(str, value)
          end
        end

        EXPLAIN_NONBOOLEAN_OPTIONS.each do |key, e_opts|
          if value = opts[key]
            if str = e_opts[value]
              add_opt.call(str, true)
            else
              raise Sequel::Error, "unrecognized value for Dataset#explain #{key.inspect} option: #{value.inspect}"
            end
          end
        end

        origin << ') ' unless paren
        origin
      end

      # Add ON CONFLICT clause if it should be used
      def insert_conflict_sql(sql)
        if opts = @opts[:insert_conflict]
          sql << " ON CONFLICT"

          if target = opts[:constraint] 
            sql << " ON CONSTRAINT "
            identifier_append(sql, target)
          elsif target = opts[:target]
            sql << ' '
            identifier_append(sql, Array(target))
            if conflict_where = opts[:conflict_where]
              sql << " WHERE "
              literal_append(sql, conflict_where)
            end
          end

          if values = opts[:update]
            sql << " DO UPDATE SET "
            update_sql_values_hash(sql, values)
            if update_where = opts[:update_where]
              sql << " WHERE "
              literal_append(sql, update_where)
            end
          else
            sql << " DO NOTHING"
          end
        end
      end

      # Include aliases when inserting into a single table on PostgreSQL 9.5+.
      def insert_into_sql(sql)
        sql << " INTO "
        if (f = @opts[:from]) && f.length == 1
          identifier_append(sql, server_version >= 90500 ? f.first : unaliased_identifier(f.first))
        else
          source_list_append(sql, f)
        end
      end

      # Return the primary key to use for RETURNING in an INSERT statement
      def insert_pk
        (f = opts[:from]) && !f.empty? && (t = f.first)

        t = t.call(self) if t.is_a? Sequel::SQL::DelayedEvaluation

        case t
        when Symbol, String, SQL::Identifier, SQL::QualifiedIdentifier
          if pk = db.primary_key(t)
            Sequel::SQL::Identifier.new(pk)
          end
        end
      end

      # Support OVERRIDING SYSTEM|USER VALUE in insert statements
      def insert_override_sql(sql)
        case opts[:override]
        when :system
          sql << " OVERRIDING SYSTEM VALUE"
        when :user
          sql << " OVERRIDING USER VALUE"
        end
      end

      # For multiple table support, PostgreSQL requires at least
      # two from tables, with joins allowed.
      def join_from_sql(type, sql)
        if(from = @opts[:from][1..-1]).empty?
          raise(Error, 'Need multiple FROM tables if updating/deleting a dataset with JOINs') if @opts[:join]
        else
          sql << ' ' << type.to_s << ' '
          source_list_append(sql, from)
          select_join_sql(sql)
        end
      end

      # Support table aliases for USING columns
      def join_using_clause_using_sql_append(sql, using_columns)
        if using_columns.is_a?(SQL::AliasedExpression)
          super(sql, using_columns.expression)
          sql << ' AS '
          identifier_append(sql, using_columns.alias)
        else
          super
        end
      end
    
      # Use a generic blob quoting method, hopefully overridden in one of the subadapter methods
      def literal_blob_append(sql, v)
        sql << "'" << v.gsub(/[\000-\037\047\134\177-\377]/n){|b| "\\#{("%o" % b[0..1].unpack("C")[0]).rjust(3, '0')}"} << "'"
      end

      # PostgreSQL uses FALSE for false values
      def literal_false
        'false'
      end
      
      # PostgreSQL quotes NaN and Infinity.
      def literal_float(value)
        if value.finite?
          super
        elsif value.nan?
          "'NaN'"
        elsif value.infinite? == 1
          "'Infinity'"
        else
          "'-Infinity'"
        end
      end 

      # Handle Ruby integers outside PostgreSQL bigint range specially.
      def literal_integer(v)
        if v > 9223372036854775807 || v < -9223372036854775808
          literal_integer_outside_bigint_range(v)
        else
          v.to_s
        end
      end

      # Raise IntegerOutsideBigintRange when attempting to literalize Ruby integer
      # outside PostgreSQL bigint range, so PostgreSQL doesn't treat
      # the value as numeric.
      def literal_integer_outside_bigint_range(v)
        raise IntegerOutsideBigintRange, "attempt to literalize Ruby integer outside PostgreSQL bigint range: #{v}"
      end

      # Assume that SQL standard quoting is on, per Sequel's defaults
      def literal_string_append(sql, v)
        sql << "'" << v.gsub("'", "''") << "'"
      end

      # PostgreSQL uses true for true values
      def literal_true
        'true'
      end

      # PostgreSQL supports multiple rows in INSERT.
      def multi_insert_sql_strategy
        :values
      end

      # Dataset options that do not affect the generated SQL.
      def non_sql_option?(key)
        super || key == :cursor || key == :insert_conflict
      end

      # PostgreSQL requires parentheses around compound datasets if they use
      # CTEs, and using them in other places doesn't hurt.
      def compound_dataset_sql_append(sql, ds)
        sql << '('
        super
        sql << ')'
      end

      # Backslash is supported by default as the escape character on PostgreSQL,
      # and using ESCAPE can break LIKE ANY() usage.
      def requires_like_escape?
        false
      end

      # Support FETCH FIRST WITH TIES on PostgreSQL 13+.
      def select_limit_sql(sql)
        l = @opts[:limit]
        o = @opts[:offset]

        return unless l || o

        if @opts[:limit_with_ties]
          if o
            sql << " OFFSET "
            literal_append(sql, o)
          end

          if l
            sql << " FETCH FIRST "
            literal_append(sql, l)
            sql << " ROWS WITH TIES"
          end
        else
          if l
            sql << " LIMIT "
            literal_append(sql, l)
          end

          if o
            sql << " OFFSET "
            literal_append(sql, o)
          end
        end
      end

      # Support FOR SHARE locking when using the :share lock style.
      # Use SKIP LOCKED if skipping locked rows.
      def select_lock_sql(sql)
        lock = @opts[:lock]
        case lock
        when :share
          sql << ' FOR SHARE'
        when :no_key_update
          sql << ' FOR NO KEY UPDATE'
        when :key_share
          sql << ' FOR KEY SHARE'
        else
          super
        end

        if lock
          if @opts[:skip_locked]
            sql << " SKIP LOCKED"
          elsif @opts[:nowait]
            sql << " NOWAIT"
          end
        end
      end

      # Support VALUES clause instead of the SELECT clause to return rows.
      def select_values_sql(sql)
        sql << "VALUES "
        expression_list_append(sql, opts[:values])
      end

      # Use WITH RECURSIVE instead of WITH if any of the CTEs is recursive
      def select_with_sql_base
        opts[:with].any?{|w| w[:recursive]} ? "WITH RECURSIVE " : super
      end

      # Support PostgreSQL 14+ CTE SEARCH/CYCLE clauses
      def select_with_sql_cte(sql, cte)
        super
        select_with_sql_cte_search_cycle(sql, cte)
      end

      def select_with_sql_cte_search_cycle(sql, cte)
        if search_opts = cte[:search]
          sql << if search_opts[:type] == :breadth
            " SEARCH BREADTH FIRST BY "
          else
            " SEARCH DEPTH FIRST BY "
          end

          identifier_list_append(sql, Array(search_opts[:by]))
          sql << " SET "
          identifier_append(sql, search_opts[:set] || :ordercol)
        end

        if cycle_opts = cte[:cycle]
          sql << " CYCLE "
          identifier_list_append(sql, Array(cycle_opts[:columns]))
          sql << " SET "
          identifier_append(sql, cycle_opts[:cycle_column] || :is_cycle)
          if cycle_opts.has_key?(:cycle_value)
            sql << " TO "
            literal_append(sql, cycle_opts[:cycle_value])
            sql << " DEFAULT "
            literal_append(sql, cycle_opts.fetch(:noncycle_value, false))
          end
          sql << " USING "
          identifier_append(sql, cycle_opts[:path_column] || :path)
        end
      end

      # The version of the database server
      def server_version
        db.server_version(@opts[:server])
      end

      # PostgreSQL 9.4+ supports the FILTER clause for aggregate functions.
      def supports_filtered_aggregates?
        server_version >= 90400
      end

      # PostgreSQL supports quoted function names.
      def supports_quoted_function_names?
        true
      end

      # Concatenate the expressions with a space in between
      def full_text_string_join(cols)
        cols = Array(cols).map{|x| SQL::Function.new(:COALESCE, x, '')}
        cols = cols.zip([' '] * cols.length).flatten
        cols.pop
        SQL::StringExpression.new(:'||', *cols)
      end

      # Use FROM to specify additional tables in an update query
      def update_from_sql(sql)
        join_from_sql(:FROM, sql)
      end

      # Only include the primary table in the main update clause
      def update_table_sql(sql)
        sql << ' '
        source_list_append(sql, @opts[:from][0..0])
      end
    end
  end
end
