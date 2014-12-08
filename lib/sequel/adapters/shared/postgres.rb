Sequel.require 'adapters/utils/pg_types'

module Sequel
  # Top level module for holding all PostgreSQL-related modules and classes
  # for Sequel.  There are a few module level accessors that are added via
  # metaprogramming.  These are:
  #
  # client_min_messages :: Change the minimum level of messages that PostgreSQL will send to the
  #                        the client.  The PostgreSQL default is NOTICE, the Sequel default is
  #                        WARNING.  Set to nil to not change the server default. Overridable on
  #                        a per instance basis via the :client_min_messages option.
  # force_standard_strings :: Set to false to not force the use of standard strings.  Overridable
  #                           on a per instance basis via the :force_standard_strings option.
  #
  # It is not recommened you use these module-level accessors.  Instead,
  # use the database option to make the setting per-Database.
  #
  # All adapters that connect to PostgreSQL support the following option in
  # addition to those mentioned above:
  #
  # :search_path :: Set the schema search_path for this Database's connections.
  #                 Allows to to set which schemas do not need explicit
  #                 qualification, and in which order to check the schemas when
  #                 an unqualified object is referenced.
  module Postgres
    # Array of exceptions that need to be converted.  JDBC
    # uses NativeExceptions, the native adapter uses PGError.
    CONVERTED_EXCEPTIONS = []

    @client_min_messages = :warning
    @force_standard_strings = true

    class << self
      # By default, Sequel sets the minimum level of log messages sent to the client
      # to WARNING, where PostgreSQL uses a default of NOTICE.  This is to avoid a lot
      # of mostly useless messages when running migrations, such as a couple of lines
      # for every serial primary key field.
      attr_accessor :client_min_messages

      # By default, Sequel forces the use of standard strings, so that
      # '\\' is interpreted as \\ and not \.  While PostgreSQL <9.1 defaults
      # to interpreting plain strings, newer versions use standard strings by
      # default.  Sequel assumes that SQL standard strings will be used.  Setting
      # this to false means Sequel will use the database's default.
      attr_accessor :force_standard_strings
    end

    class CreateTableGenerator < Sequel::Schema::Generator
      # Add an exclusion constraint when creating the table. Elements should be
      # an array of 2 element arrays, with the first element being the column or
      # expression the exclusion constraint is applied to, and the second element
      # being the operator to use for the column/expression to check for exclusion.
      #
      # Example:
      #
      #   exclude([[:col1, '&&'], [:col2, '=']])
      #   # EXCLUDE USING gist (col1 WITH &&, col2 WITH =)
      #
      # Options supported:
      #
      # :name :: Name the constraint with the given name (useful if you may
      #          need to drop the constraint later)
      # :using :: Override the index_method for the exclusion constraint (defaults to gist).
      # :where :: Create a partial exclusion constraint, which only affects
      #           a subset of table rows, value should be a filter expression.
      def exclude(elements, opts=OPTS)
        constraints << {:type => :exclude, :elements => elements}.merge(opts)
      end
    end

    class AlterTableGenerator < Sequel::Schema::AlterTableGenerator
      # Adds an exclusion constraint to an existing table, see
      # CreateTableGenerator#exclude.
      def add_exclusion_constraint(elements, opts=OPTS)
        @operations << {:op => :add_constraint, :type => :exclude, :elements => elements}.merge(opts)
      end

      # Validate the constraint with the given name, which should have
      # been added previously with NOT VALID.
      def validate_constraint(name)
        @operations << {:op => :validate_constraint, :name => name}
      end
    end

    # Error raised when Sequel determines a PostgreSQL exclusion constraint has been violated.
    class ExclusionConstraintViolation < Sequel::ConstraintViolation; end

    # Methods shared by Database instances that connect to PostgreSQL.
    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      PREPARED_ARG_PLACEHOLDER = LiteralString.new('$').freeze
      RE_CURRVAL_ERROR = /currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/.freeze
      FOREIGN_KEY_LIST_ON_DELETE_MAP = {'a'.freeze=>:no_action, 'r'.freeze=>:restrict, 'c'.freeze=>:cascade, 'n'.freeze=>:set_null, 'd'.freeze=>:set_default}.freeze
      POSTGRES_DEFAULT_RE = /\A(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))\z/
      UNLOGGED = 'UNLOGGED '.freeze
      ON_COMMIT = {
        :drop => 'DROP', :delete_rows => 'DELETE ROWS', :preserve_rows => 'PRESERVE ROWS',
      }.freeze

      # SQL fragment for custom sequences (ones not created by serial primary key),
      # Returning the schema and literal form of the sequence name, by parsing
      # the column defaults table.
      SELECT_CUSTOM_SEQUENCE_SQL = (<<-end_sql
        SELECT name.nspname AS "schema",
            CASE
            WHEN split_part(def.adsrc, '''', 2) ~ '.' THEN
              substr(split_part(def.adsrc, '''', 2),
                     strpos(split_part(def.adsrc, '''', 2), '.')+1)
            ELSE split_part(def.adsrc, '''', 2)
          END AS "sequence"
        FROM pg_class t
        JOIN pg_namespace  name ON (t.relnamespace = name.oid)
        JOIN pg_attribute  attr ON (t.oid = attrelid)
        JOIN pg_attrdef    def  ON (adrelid = attrelid AND adnum = attnum)
        JOIN pg_constraint cons ON (conrelid = adrelid AND adnum = conkey[1])
        WHERE cons.contype = 'p'
          AND def.adsrc ~* 'nextval'
      end_sql
      ).strip.gsub(/\s+/, ' ').freeze

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
      ).strip.gsub(/\s+/, ' ').freeze

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
      ).strip.gsub(/\s+/, ' ').freeze

      # A hash of conversion procs, keyed by type integer (oid) and
      # having callable values for the conversion proc for that type.
      attr_reader :conversion_procs

      # Commit an existing prepared transaction with the given transaction
      # identifier string.
      def commit_prepared_transaction(transaction_id, opts=OPTS)
        run("COMMIT PREPARED #{literal(transaction_id)}", opts)
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
      #         :when :: A filter to use for the trigger
      def create_trigger(table, name, function, opts=OPTS)
        self << create_trigger_sql(table, name, function, opts)
      end

      # PostgreSQL uses the :postgres database type.
      def database_type
        :postgres
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
      def foreign_key_list(table, opts=OPTS)
        m = output_identifier_meth
        schema, _ = opts.fetch(:schema, schema_and_table(table))
        range = 0...32

        base_ds = metadata_dataset.
          from(:pg_constraint___co).
          join(:pg_class___cl, :oid=>:conrelid).
          where(:cl__relkind=>'r', :co__contype=>'f', :cl__oid=>regclass_oid(table))

        # We split the parsing into two separate queries, which are merged manually later.
        # This is because PostgreSQL stores both the referencing and referenced columns in
        # arrays, and I don't know a simple way to not create a cross product, as PostgreSQL
        # doesn't appear to have a function that takes an array and element and gives you
        # the index of that element in the array.

        ds = base_ds.
          join(:pg_attribute___att, :attrelid=>:oid, :attnum=>SQL::Function.new(:ANY, :co__conkey)).
          order(:co__conname, SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(:co__conkey, [x]), x]}, 32, :att__attnum)).
          select(:co__conname___name, :att__attname___column, :co__confupdtype___on_update, :co__confdeltype___on_delete,
                 SQL::BooleanExpression.new(:AND, :co__condeferrable, :co__condeferred).as(:deferrable))

        ref_ds = base_ds.
          join(:pg_class___cl2, :oid=>:co__confrelid).
          join(:pg_attribute___att2, :attrelid=>:oid, :attnum=>SQL::Function.new(:ANY, :co__confkey)).
          order(:co__conname, SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(:co__conkey, [x]), x]}, 32, :att2__attnum)).
          select(:co__conname___name, :cl2__relname___table, :att2__attname___refcolumn)

        # If a schema is given, we only search in that schema, and the returned :table
        # entry is schema qualified as well.
        if schema
          ref_ds = ref_ds.join(:pg_namespace___nsp2, :oid=>:cl2__relnamespace).
            select_more(:nsp2__nspname___schema)
        end

        h = {}
        fklod_map = FOREIGN_KEY_LIST_ON_DELETE_MAP 
        ds.each do |row|
          if r = h[row[:name]]
            r[:columns] << m.call(row[:column])
          else
            h[row[:name]] = {:name=>m.call(row[:name]), :columns=>[m.call(row[:column])], :on_update=>fklod_map[row[:on_update]], :on_delete=>fklod_map[row[:on_delete]], :deferrable=>row[:deferrable]}
          end
        end
        ref_ds.each do |row|
          r = h[row[:name]]
          r[:table] ||= schema ? SQL::QualifiedIdentifier.new(m.call(row[:schema]), m.call(row[:table])) : m.call(row[:table])
          r[:key] ||= []
          r[:key] << m.call(row[:refcolumn])
        end
        h.values
      end

      # Use the pg_* system tables to determine indexes on a table
      def indexes(table, opts=OPTS)
        m = output_identifier_meth
        range = 0...32
        attnums = server_version >= 80100 ? SQL::Function.new(:ANY, :ind__indkey) : range.map{|x| SQL::Subscript.new(:ind__indkey, [x])}
        ds = metadata_dataset.
          from(:pg_class___tab).
          join(:pg_index___ind, :indrelid=>:oid).
          join(:pg_class___indc, :oid=>:indexrelid).
          join(:pg_attribute___att, :attrelid=>:tab__oid, :attnum=>attnums).
          left_join(:pg_constraint___con, :conname=>:indc__relname).
          filter(:indc__relkind=>'i', :ind__indisprimary=>false, :indexprs=>nil, :indpred=>nil, :indisvalid=>true, :tab__oid=>regclass_oid(table, opts)).
          order(:indc__relname, SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(:ind__indkey, [x]), x]}, 32, :att__attnum)).
          select(:indc__relname___name, :ind__indisunique___unique, :att__attname___column, :con__condeferrable___deferrable)

        ds.filter!(:indisready=>true, :indcheckxmin=>false) if server_version >= 80300

        indexes = {}
        ds.each do |r|
          i = indexes[m.call(r[:name])] ||= {:columns=>[], :unique=>r[:unique], :deferrable=>r[:deferrable]}
          i[:columns] << m.call(r[:column])
        end
        indexes
      end

      # Dataset containing all current database locks
      def locks
        dataset.from(:pg_class).join(:pg_locks, :relation=>:relfilenode).select(:pg_class__relname, Sequel::SQL::ColumnAll.new(:pg_locks))
      end

      # Notifies the given channel.  See the PostgreSQL NOTIFY documentation. Options:
      #
      # :payload :: The payload string to use for the NOTIFY statement.  Only supported
      #             in PostgreSQL 9.0+.
      # :server :: The server to which to send the NOTIFY statement, if the sharding support
      #            is being used.
      def notify(channel, opts=OPTS)
        sql = "NOTIFY "
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
        sql = "#{SELECT_PK_SQL} AND pg_class.oid = #{literal(regclass_oid(table, opts))}"
        value = fetch(sql).single_value
        Sequel.synchronize{@primary_keys[quoted_table] = value}
      end

      # Return the sequence providing the default for the primary key for the given table.
      def primary_key_sequence(table, opts=OPTS)
        quoted_table = quote_schema_table(table)
        Sequel.synchronize{return @primary_key_sequences[quoted_table] if @primary_key_sequences.has_key?(quoted_table)}
        sql = "#{SELECT_SERIAL_SEQUENCE_SQL} AND t.oid = #{literal(regclass_oid(table, opts))}"
        if pks = fetch(sql).single_record
          value = literal(SQL::QualifiedIdentifier.new(pks[:schema], pks[:sequence]))
          Sequel.synchronize{@primary_key_sequences[quoted_table] = value}
        else
          sql = "#{SELECT_CUSTOM_SEQUENCE_SQL} AND t.oid = #{literal(regclass_oid(table, opts))}"
          if pks = fetch(sql).single_record
            value = literal(SQL::QualifiedIdentifier.new(pks[:schema], LiteralString.new(pks[:sequence])))
            Sequel.synchronize{@primary_key_sequences[quoted_table] = value}
          end
        end
      end

      # Refresh the materialized view with the given name.
      # 
      #   DB.refresh_view(:items_view)
      #   # REFRESH MATERIALIZED VIEW items_view
      #   DB.refresh_view(:items_view, :concurrently=>true)
      #   # REFRESH MATERIALIZED VIEW CONCURRENTLY items_view
      def refresh_view(name, opts=OPTS)
        run "REFRESH MATERIALIZED VIEW#{' CONCURRENTLY' if opts[:concurrently]} #{quote_schema_table(name)}"
      end
      
      # Reset the database's conversion procs, requires a server query if there
      # any named types.
      def reset_conversion_procs
        @conversion_procs = get_conversion_procs
        conversion_procs_updated
        @conversion_procs
      end

      # Reset the primary key sequence for the given table, basing it on the
      # maximum current value of the table's primary key.
      def reset_primary_key_sequence(table)
        return unless seq = primary_key_sequence(table)
        pk = SQL::Identifier.new(primary_key(table))
        db = self
        seq_ds = db.from(LiteralString.new(seq))
        s, t = schema_and_table(table)
        table = Sequel.qualify(s, t) if s
        get{setval(seq, db[table].select{coalesce(max(pk)+seq_ds.select{:increment_by}, seq_ds.select(:min_value))}, false)}
      end

      # Rollback an existing prepared transaction with the given transaction
      # identifier string.
      def rollback_prepared_transaction(transaction_id, opts=OPTS)
        run("ROLLBACK PREPARED #{literal(transaction_id)}", opts)
      end

      # PostgreSQL uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        {:primary_key => true, :serial => true, :type=>Integer}
      end

      # The version of the PostgreSQL server, used for determining capability.
      def server_version(server=nil)
        return @server_version if @server_version
        @server_version = synchronize(server) do |conn|
          (conn.server_version rescue nil) if conn.respond_to?(:server_version)
        end
        unless @server_version
          @server_version = if m = /PostgreSQL (\d+)\.(\d+)(?:(?:rc\d+)|\.(\d+))?/.match(fetch('SELECT version()').single_value)
            (m[1].to_i * 10000) + (m[2].to_i * 100) + m[3].to_i
          else
            0
          end
        end
        warn 'Sequel no longer supports PostgreSQL <8.2, some things may not work' if @server_version < 80200
        @server_version
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
        pg_class_relname('r', opts, &block)
      end

      # Check whether the given type name string/symbol (e.g. :hstore) is supported by
      # the database.
      def type_supported?(type)
        @supported_types ||= {}
        @supported_types.fetch(type){@supported_types[type] = (from(:pg_type).filter(:typtype=>'b', :typname=>type.to_s).count > 0)}
      end

      # Creates a dataset that uses the VALUES clause:
      #
      #   DB.values([[1, 2], [3, 4]])
      #   VALUES ((1, 2), (3, 4))
      #
      #   DB.values([[1, 2], [3, 4]]).order(:column2).limit(1, 1)
      #   VALUES ((1, 2), (3, 4)) ORDER BY column2 LIMIT 1 OFFSET 1
      def values(v)
        @default_dataset.clone(:values=>v)
      end

      # Array of symbols specifying view names in the current database.
      #
      # Options:
      # :qualify :: Return the views as Sequel::SQL::QualifiedIdentifier instances,
      #             using the schema the view is located in as the qualifier.
      # :schema :: The schema to search
      # :server :: The server to use
      def views(opts=OPTS)
        pg_class_relname('v', opts)
      end

      private

      # Do a type name-to-oid lookup using the database and update the procs
      # with the related proc if the database supports the type.
      def add_named_conversion_procs(procs, named_procs)
        unless (named_procs).empty?
          convert_named_procs_to_procs(named_procs).each do |oid, pr|
            procs[oid] ||= pr
          end
          conversion_procs_updated
        end
      end

      # Use a PostgreSQL-specific alter table generator
      def alter_table_generator_class
        Postgres::AlterTableGenerator
      end
    
      # Handle :using option for set_column_type op, and the :validate_constraint op.
      def alter_table_op_sql(table, op)
        case op[:op]
        when :set_column_type
          s = super
          if using = op[:using]
            using = Sequel::LiteralString.new(using) if using.is_a?(String)
            s << ' USING '
            s << literal(using)
          end
          s
        when :validate_constraint
          "VALIDATE CONSTRAINT #{quote_identifier(op[:name])}"
        else
          super
        end
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

      # Handle PostgreSQL specific default format.
      def column_schema_normalize_default(default, type)
        if m = POSTGRES_DEFAULT_RE.match(default)
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
      # the custom validate_constraint operation.
      def combinable_alter_table_op?(op)
        (super || op[:op] == :validate_constraint) && op[:op] != :rename_column
      end

      VALID_CLIENT_MIN_MESSAGES = %w'DEBUG5 DEBUG4 DEBUG3 DEBUG2 DEBUG1 LOG NOTICE WARNING ERROR FATAL PANIC'.freeze
      # The SQL queries to execute when starting a new connection.
      def connection_configuration_sqls
        sqls = []

        sqls << "SET standard_conforming_strings = ON" if typecast_value_boolean(@opts.fetch(:force_standard_strings, Postgres.force_standard_strings))

        if (cmm = @opts.fetch(:client_min_messages, Postgres.client_min_messages)) && !cmm.to_s.empty?
          cmm = cmm.to_s.upcase.strip
          unless VALID_CLIENT_MIN_MESSAGES.include?(cmm)
            raise Error, "Unsupported client_min_messages setting: #{cmm}"
          end
          sqls << "SET client_min_messages = '#{cmm.to_s.upcase}'"
        end

        if search_path = @opts[:search_path]
          case search_path
          when String
            search_path = search_path.split(",").map{|s| s.strip}
          when Array
            # nil
          else
            raise Error, "unrecognized value for :search_path option: #{search_path.inspect}"
          end
          sqls << "SET search_path = #{search_path.map{|s| "\"#{s.gsub('"', '""')}\""}.join(',')}"
        end

        sqls
      end

      # Handle exclusion constraints.
      def constraint_definition_sql(constraint)
        case constraint[:type]
        when :exclude
          elements = constraint[:elements].map{|c, op| "#{literal(c)} WITH #{op}"}.join(', ')
          sql = "#{"CONSTRAINT #{quote_identifier(constraint[:name])} " if constraint[:name]}EXCLUDE USING #{constraint[:using]||'gist'} (#{elements})#{" WHERE #{filter_expr(constraint[:where])}" if constraint[:where]}"
          constraint_deferrable_sql_append(sql, constraint[:deferrable])
          sql
        when :foreign_key, :check
          sql = super
          if constraint[:not_valid]
            sql << " NOT VALID"
          end
          sql
        else
          super
        end
      end

      # Callback used when conversion procs are updated.
      def conversion_procs_updated
        nil
      end

      # Convert the hash of named conversion procs into a hash a oid conversion procs. 
      def convert_named_procs_to_procs(named_procs)
        h = {}
        from(:pg_type).where(:typtype=>'b', :typname=>named_procs.keys.map{|t| t.to_s}).select_map([:oid, :typname]).each do |oid, name|
          h[oid.to_i] = named_procs[name.untaint.to_sym]
        end
        h
      end

      # Copy the conversion procs related to the given oids from PG_TYPES into
      # the conversion procs for this instance.
      def copy_conversion_procs(oids)
        procs = conversion_procs
        oids.each do |oid|
          procs[oid] = PG_TYPES[oid]
        end
        conversion_procs_updated
      end

      EXCLUSION_CONSTRAINT_SQL_STATE = '23P01'.freeze
      DEADLOCK_SQL_STATE = '40P01'.freeze
      def database_specific_error_class_from_sqlstate(sqlstate)
        if sqlstate == EXCLUSION_CONSTRAINT_SQL_STATE
          ExclusionConstraintViolation
        elsif sqlstate == DEADLOCK_SQL_STATE
          SerializationFailure
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
      ].freeze
      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # SQL for doing fast table insert from stdin.
      def copy_into_sql(table, opts)
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
        sql
      end

      # SQL for doing fast table output to stdout.
      def copy_table_sql(table, opts)
        if table.is_a?(String)
          table
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
          "COPY #{table} TO STDOUT#{options}"
        end
      end

      # SQL statement to create database function.
      def create_function_sql(name, definition, opts=OPTS)
        args = opts[:args]
        if !opts[:args].is_a?(Array) || !opts[:args].any?{|a| Array(a).length == 3 and %w'OUT INOUT'.include?(a[2].to_s)}
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
        elsif options[:unlogged]
          UNLOGGED
        end

        "CREATE #{prefix_sql}TABLE#{' IF NOT EXISTS' if options[:if_not_exists]} #{options[:temp] ? quote_identifier(name) : quote_schema_table(name)}"
      end

      def create_table_sql(name, generator, options)
        sql = super

        if inherits = options[:inherits]
          sql << " INHERITS (#{Array(inherits).map{|t| quote_schema_table(t)}.join(', ')})"
        end

        if on_commit = options[:on_commit]
          raise(Error, "can't provide :on_commit without :temp to create_table") unless options[:temp]
          raise(Error, "unsupported on_commit option: #{on_commit.inspect}") unless ON_COMMIT.has_key?(on_commit)
          sql << " ON COMMIT #{ON_COMMIT[on_commit]}"
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
          result << " ON COMMIT #{ON_COMMIT[on_commit]}"
        end
        result << " AS #{sql}"
      end

      # Use a PostgreSQL-specific create table generator
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
        "CREATE TRIGGER #{name} #{whence} #{events.map{|e| e.to_s.upcase}.join(' OR ')} ON #{quote_schema_table(table)}#{' FOR EACH ROW' if opts[:each_row]}#{filter} EXECUTE PROCEDURE #{function}(#{Array(opts[:args]).map{|a| literal(a)}.join(', ')})"
      end

      # DDL fragment for initial part of CREATE VIEW statement
      def create_view_prefix_sql(name, options)
        create_view_sql_append_columns("CREATE #{'OR REPLACE 'if options[:replace]}#{'TEMPORARY 'if options[:temp]}#{'RECURSIVE ' if options[:recursive]}#{'MATERIALIZED ' if options[:materialized]}VIEW #{quote_schema_table(name)}", options[:columns] || options[:recursive])
      end

      # The errors that the main adapters can raise, depends on the adapter being used
      def database_error_classes
        CONVERTED_EXCEPTIONS
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

      # If opts includes a :schema option, or a default schema is used, restrict the dataset to
      # that schema.  Otherwise, just exclude the default PostgreSQL schemas except for public.
      def filter_schema(ds, opts)
        expr = if schema = opts[:schema]
          schema.to_s
        else
          Sequel.function(:any, Sequel.function(:current_schemas, false))
        end
        ds.where(:pg_namespace__nspname=>expr)
      end

      # Return a hash with oid keys and callable values, used for converting types.
      def get_conversion_procs
        procs = PG_TYPES.dup
        procs[1184] = procs[1114] = method(:to_application_timestamp)
        add_named_conversion_procs(procs, PG_NAMED_TYPES)
        procs
      end

      # PostgreSQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end

      # PostgreSQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      # PostgreSQL specific index SQL.
      def index_definition_sql(table_name, index)
        cols = index[:columns]
        index_name = index[:name] || default_index_name(table_name, cols)
        expr = if o = index[:opclass]
          "(#{Array(cols).map{|c| "#{literal(c)} #{o}"}.join(', ')})"
        else
          literal(Array(cols))
        end
        unique = "UNIQUE " if index[:unique]
        index_type = index[:type]
        filter = index[:where] || index[:filter]
        filter = " WHERE #{filter_expr(filter)}" if filter
        case index_type
        when :full_text
          expr = "(to_tsvector(#{literal(index[:language] || 'simple')}::regconfig, #{literal(dataset.send(:full_text_string_join, cols))}))"
          index_type = index[:index_type] || :gin
        when :spatial
          index_type = :gist
        end
        "CREATE #{unique}INDEX#{' CONCURRENTLY' if index[:concurrently]} #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{"USING #{index_type} " if index_type}#{expr}#{filter}"
      end

      # Setup datastructures shared by all postgres adapters.
      def initialize_postgres_adapter
        @primary_keys = {}
        @primary_key_sequences = {}
        @conversion_procs = PG_TYPES.dup
        reset_conversion_procs
      end

      # Backbone of the tables and views support.
      def pg_class_relname(type, opts)
        ds = metadata_dataset.from(:pg_class).filter(:relkind=>type).select(:relname).server(opts[:server]).join(:pg_namespace, :oid=>:relnamespace)
        ds = filter_schema(ds, opts)
        m = output_identifier_meth
        if block_given?
          yield(ds)
        elsif opts[:qualify]
          ds.select_append(:pg_namespace__nspname).map{|r| Sequel.qualify(m.call(r[:nspname]), m.call(r[:relname]))}
        else
          ds.map{|r| m.call(r[:relname])}
        end
      end

      # Use a dollar sign instead of question mark for the argument
      # placeholder.
      def prepared_arg_placeholder
        PREPARED_ARG_PLACEHOLDER
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

      # Remove the cached entries for primary keys and sequences when a table is
      # changed.
      def remove_cached_schema(table)
        tab = quote_schema_table(table)
        Sequel.synchronize do
          @primary_keys.delete(tab)
          @primary_key_sequences.delete(tab)
        end
        super
      end

      # SQL DDL statement for renaming a table. PostgreSQL doesn't allow you to change a table's schema in
      # a rename table operation, so speciying a new schema in new_name will not have an effect.
      def rename_table_sql(name, new_name)
        "ALTER TABLE #{quote_schema_table(name)} RENAME TO #{quote_identifier(schema_and_table(new_name).last)}"
      end

      # Recognize PostgreSQL interval type.
      def schema_column_type(db_type)
        case db_type
        when /\Ainterval\z/io
          :interval
        when /\Acitext\z/io
          :string
        else
          super
        end
      end

      # The dataset used for parsing table schemas, using the pg_* system catalogs.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])
        ds = metadata_dataset.select(:pg_attribute__attname___name,
            SQL::Cast.new(:pg_attribute__atttypid, :integer).as(:oid),
            SQL::Cast.new(:basetype__oid, :integer).as(:base_oid),
            SQL::Function.new(:format_type, :basetype__oid, :pg_type__typtypmod).as(:db_base_type),
            SQL::Function.new(:format_type, :pg_type__oid, :pg_attribute__atttypmod).as(:db_type),
            SQL::Function.new(:pg_get_expr, :pg_attrdef__adbin, :pg_class__oid).as(:default),
            SQL::BooleanExpression.new(:NOT, :pg_attribute__attnotnull).as(:allow_null),
            SQL::Function.new(:COALESCE, SQL::BooleanExpression.from_value_pairs(:pg_attribute__attnum => SQL::Function.new(:ANY, :pg_index__indkey)), false).as(:primary_key)).
          from(:pg_class).
          join(:pg_attribute, :attrelid=>:oid).
          join(:pg_type, :oid=>:atttypid).
          left_outer_join(:pg_type___basetype, :oid=>:typbasetype).
          left_outer_join(:pg_attrdef, :adrelid=>:pg_class__oid, :adnum=>:pg_attribute__attnum).
          left_outer_join(:pg_index, :indrelid=>:pg_class__oid, :indisprimary=>true).
          filter(:pg_attribute__attisdropped=>false).
          filter{|o| o.pg_attribute__attnum > 0}.
          filter(:pg_class__oid=>regclass_oid(table_name, opts)).
          order(:pg_attribute__attnum)
        ds.map do |row|
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
          row[:type] = schema_column_type(row[:db_type])
          if row[:primary_key]
            row[:auto_increment] = !!(row[:default] =~ /\Anextval/io)
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
          sql = "SET TRANSACTION"
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
      def type_literal_generic_bignum(column)
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
      # Otherwise use the type type.
      def type_literal_generic_string(column)
        if column[:fixed]
          "char(#{column[:size]||255})"
        elsif column[:text] == false or column[:size]
          "varchar(#{column[:size]||255})"
        else
          :text
        end
      end

      # PostgreSQL 9.4+ supports views with check option.
      def view_with_check_option_support
        :local if server_version >= 90400
      end
    end

    # Instance methods for datasets that connect to a PostgreSQL database.
    module DatasetMethods
      ACCESS_SHARE = 'ACCESS SHARE'.freeze
      ACCESS_EXCLUSIVE = 'ACCESS EXCLUSIVE'.freeze
      BOOL_FALSE = 'false'.freeze
      BOOL_TRUE = 'true'.freeze
      COMMA_SEPARATOR = ', '.freeze
      EXCLUSIVE = 'EXCLUSIVE'.freeze
      EXPLAIN = 'EXPLAIN '.freeze
      EXPLAIN_ANALYZE = 'EXPLAIN ANALYZE '.freeze
      FOR_SHARE = ' FOR SHARE'.freeze
      NULL = LiteralString.new('NULL').freeze
      PG_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
      ROW_EXCLUSIVE = 'ROW EXCLUSIVE'.freeze
      ROW_SHARE = 'ROW SHARE'.freeze
      SHARE = 'SHARE'.freeze
      SHARE_ROW_EXCLUSIVE = 'SHARE ROW EXCLUSIVE'.freeze
      SHARE_UPDATE_EXCLUSIVE = 'SHARE UPDATE EXCLUSIVE'.freeze
      SQL_WITH_RECURSIVE = "WITH RECURSIVE ".freeze
      SPACE = Dataset::SPACE
      FROM = Dataset::FROM
      APOS = Dataset::APOS
      APOS_RE = Dataset::APOS_RE
      DOUBLE_APOS = Dataset::DOUBLE_APOS
      PAREN_OPEN = Dataset::PAREN_OPEN
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      COMMA = Dataset::COMMA
      ESCAPE = Dataset::ESCAPE
      BACKSLASH = Dataset::BACKSLASH
      AS = Dataset::AS
      XOR_OP = ' # '.freeze
      CRLF = "\r\n".freeze
      BLOB_RE = /[\000-\037\047\134\177-\377]/n.freeze
      WINDOW = " WINDOW ".freeze
      SELECT_VALUES = "VALUES ".freeze
      EMPTY_STRING = ''.freeze
      LOCK_MODES = ['ACCESS SHARE', 'ROW SHARE', 'ROW EXCLUSIVE', 'SHARE UPDATE EXCLUSIVE', 'SHARE', 'SHARE ROW EXCLUSIVE', 'EXCLUSIVE', 'ACCESS EXCLUSIVE'].each{|s| s.freeze}

      Dataset.def_sql_method(self, :delete, [['if server_version >= 90100', %w'with delete from using where returning'], ['else', %w'delete from using where returning']])
      Dataset.def_sql_method(self, :insert, [['if server_version >= 90100', %w'with insert into columns values returning'], ['else', %w'insert into columns values returning']])
      Dataset.def_sql_method(self, :select, [['if opts[:values]', %w'values order limit'], ['elsif server_version >= 80400', %w'with select distinct columns from join where group having window compounds order limit lock'], ['else', %w'select distinct columns from join where group having compounds order limit lock']])
      Dataset.def_sql_method(self, :update, [['if server_version >= 90100', %w'with update table set from where returning'], ['else', %w'update table set from where returning']])

      # Shared methods for prepared statements when used with PostgreSQL databases.
      module PreparedStatementMethods
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
      end

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
          j = XOR_OP
          c = false
          args.each do |a|
            sql << j if c
            literal_append(sql, a)
            c ||= true
          end
        when :ILIKE, :'NOT ILIKE'
          sql << PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << SPACE << op.to_s << SPACE
          literal_append(sql, args.at(1))
          sql << ESCAPE
          literal_append(sql, BACKSLASH)
          sql << PAREN_CLOSE
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

      # Return the results of an EXPLAIN query as a string
      def explain(opts=OPTS)
        with_sql((opts[:analyze] ? EXPLAIN_ANALYZE : EXPLAIN) + select_sql).map(QUERY_PLAN).join(CRLF)
      end

      # Return a cloned dataset which will use FOR SHARE to lock returned rows.
      def for_share
        lock_style(:share)
      end

      # Run a full text search on PostgreSQL.  By default, searching for the inclusion
      # of any of the terms in any of the cols.
      #
      # Options:
      # :language :: The language to use for the search (default: 'simple')
      # :plain :: Whether a plain search should be used (default: false).  In this case,
      #           terms should be a single string, and it will do a search where cols
      #           contains all of the words in terms.  This ignores search operators in terms.
      # :phrase :: Similar to :plain, but also adding an ILIKE filter to ensure that
      #            returned rows also include the exact phrase used.
      # :rank :: Set to true to order by the rank, so that closer matches are returned first.
      def full_text_search(cols, terms, opts = OPTS)
        lang = Sequel.cast(opts[:language] || 'simple', :regconfig)
        terms = terms.join(' | ') if terms.is_a?(Array)
        columns = full_text_string_join(cols)
        query_func = (opts[:phrase] || opts[:plain]) ? :plainto_tsquery : :to_tsquery
        vector = Sequel.function(:to_tsvector, lang, columns)
        query = Sequel.function(query_func, lang, terms)

        ds = where(Sequel.lit(["(", " @@ ", ")"], vector, query))

        if opts[:phrase]
          ds = ds.grep(cols, "%#{escape_like(terms)}%", :case_insensitive=>true)
        end

        if opts[:rank]
          ds = ds.order{ts_rank_cd(vector, query)}
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

      # Insert a record returning the record inserted.  Always returns nil without
      # inserting a query if disable_insert_returning is used.
      def insert_select(*values)
        return unless supports_insert_select?
        server?(:default).with_sql_first(insert_select_sql(*values))
      end

      # The SQL to use for an insert_select, adds a RETURNING clause to the insert
      # unless the RETURNING clause is already present.
      def insert_select_sql(*values)
        ds = opts[:returning] ? self : returning
        ds.insert_sql(*values)
      end

      # Locks all tables in the dataset's FROM clause (but not in JOINs) with
      # the specified mode (e.g. 'EXCLUSIVE').  If a block is given, starts
      # a new transaction, locks the table, and yields.  If a block is not given
      # just locks the tables.  Note that PostgreSQL will probably raise an error
      # if you lock the table outside of an existing transaction.  Returns nil.
      def lock(mode, opts=OPTS)
        if block_given? # perform locking inside a transaction and yield to block
          @db.transaction(opts){lock(mode, opts); yield}
        else
          sql = 'LOCK TABLE '
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

      # True unless insert returning has been disabled for this dataset.
      def supports_insert_select?
        !@opts[:disable_insert_returning]
      end

      # PostgreSQL 9.3rc1+ supports lateral subqueries
      def supports_lateral_subqueries?
        server_version >= 90300
      end
      
      # PostgreSQL supports modifying joined datasets
      def supports_modifying_joins?
        true
      end

      # Returning is always supported.
      def supports_returning?(type)
        true
      end

      # PostgreSQL supports pattern matching via regular expressions
      def supports_regexp?
        true
      end

      # PostgreSQL supports timezones in literal timestamps
      def supports_timestamp_timezones?
        true
      end

      # PostgreSQL 8.4+ supports window functions
      def supports_window_functions?
        server_version >= 80400
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
      #   DB[:table].truncate # TRUNCATE TABLE "table"
      #   # => nil
      #   DB[:table].truncate(:cascade => true, :only=>true, :restart=>true) # TRUNCATE TABLE ONLY "table" RESTART IDENTITY CASCADE
      #   # => nil
      def truncate(opts = OPTS)
        if opts.empty?
          super()
        else
          clone(:truncate_opts=>opts).truncate
        end
      end

      # Return a clone of the dataset with an addition named window that can be referenced in window functions.
      def window(name, opts)
        clone(:window=>(@opts[:window]||[]) + [[name, SQL::Window.new(opts)]])
      end

      protected

      # If returned primary keys are requested, use RETURNING unless already set on the
      # dataset.  If RETURNING is already set, use existing returning values.  If RETURNING
      # is only set to return a single columns, return an array of just that column.
      # Otherwise, return an array of hashes.
      def _import(columns, values, opts=OPTS)
        if @opts[:returning]
          statements = multi_insert_sql(columns, values)
          @db.transaction(opts.merge(:server=>@opts[:server])) do
            statements.map{|st| returning_fetch_rows(st)}
          end.first.map{|v| v.length == 1 ? v.values.first : v}
        elsif opts[:return] == :primary_key
          returning(insert_pk)._import(columns, values, opts)
        else
          super
        end
      end

      private

      # Format TRUNCATE statement with PostgreSQL specific options.
      def _truncate_sql(table)
        to = @opts[:truncate_opts] || {}
        "TRUNCATE TABLE#{' ONLY' if to[:only]} #{table}#{' RESTART IDENTITY' if to[:restart]}#{' CASCADE' if to[:cascade]}"
      end

      # Allow truncation of multiple source tables.
      def check_truncation_allowed!
        raise(InvalidOperation, "Grouped datasets cannot be truncated") if opts[:group]
        raise(InvalidOperation, "Joined datasets cannot be truncated") if opts[:join]
      end

      # Only include the primary table in the main delete clause
      def delete_from_sql(sql)
        sql << FROM
        source_list_append(sql, @opts[:from][0..0])
      end

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
              Sequel::SQL::Identifier.new(pk)
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

      # Use a generic blob quoting method, hopefully overridden in one of the subadapter methods
      def literal_blob_append(sql, v)
        sql << APOS << v.gsub(BLOB_RE){|b| "\\#{("%o" % b[0..1].unpack("C")[0]).rjust(3, '0')}"} << APOS
      end

      # PostgreSQL uses FALSE for false values
      def literal_false
        BOOL_FALSE
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

      # Assume that SQL standard quoting is on, per Sequel's defaults
      def literal_string_append(sql, v)
        sql << APOS << v.gsub(APOS_RE, DOUBLE_APOS) << APOS
      end

      # PostgreSQL uses FALSE for false values
      def literal_true
        BOOL_TRUE
      end

      # PostgreSQL supports multiple rows in INSERT.
      def multi_insert_sql_strategy
        :values
      end

      # PostgreSQL requires parentheses around compound datasets if they use
      # CTEs, and using them in other places doesn't hurt.
      def compound_dataset_sql_append(sql, ds)
        sql << PAREN_OPEN
        super
        sql << PAREN_CLOSE
      end

      # Support FOR SHARE locking when using the :share lock style.
      def select_lock_sql(sql)
        @opts[:lock] == :share ? (sql << FOR_SHARE) : super
      end

      # Support VALUES clause instead of the SELECT clause to return rows.
      def select_values_sql(sql)
        sql << SELECT_VALUES
        expression_list_append(sql, opts[:values])
      end

      # SQL fragment for named window specifications
      def select_window_sql(sql)
        if ws = @opts[:window]
          sql << WINDOW
          c = false
          co = COMMA
          as = AS
          ws.map do |name, window|
            sql << co if c
            literal_append(sql, name)
            sql << as
            literal_append(sql, window)
            c ||= true
          end
        end
      end

      # Use WITH RECURSIVE instead of WITH if any of the CTEs is recursive
      def select_with_sql_base
        opts[:with].any?{|w| w[:recursive]} ? SQL_WITH_RECURSIVE : super
      end

      # The version of the database server
      def server_version
        db.server_version(@opts[:server])
      end

      # PostgreSQL supports quoted function names.
      def supports_quoted_function_names?
        true
      end

      # Concatenate the expressions with a space in between
      def full_text_string_join(cols)
        cols = Array(cols).map{|x| SQL::Function.new(:COALESCE, x, EMPTY_STRING)}
        cols = cols.zip([SPACE] * cols.length).flatten
        cols.pop
        SQL::StringExpression.new(:'||', *cols)
      end

      # Use FROM to specify additional tables in an update query
      def update_from_sql(sql)
        join_from_sql(:FROM, sql)
      end

      # Only include the primary table in the main update clause
      def update_table_sql(sql)
        sql << SPACE
        source_list_append(sql, @opts[:from][0..0])
      end
    end
  end
end
