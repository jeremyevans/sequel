module Sequel
  # Top level module for holding all PostgreSQL-related modules and classes
  # for Sequel.  There are a few module level accessors that are added via
  # metaprogramming.  These are:
  # * client_min_messages (only available when using the native adapter) -
  #   Change the minimum level of messages that PostgreSQL will send to the
  #   the client.  The PostgreSQL default is NOTICE, the Sequel default is
  #   WARNING.  Set to nil to not change the server default.
  # * force_standard_strings - Set to false to not force the use of
  #   standard strings
  # * use_iso_date_format (only available when using the native adapter) -
  #   Set to false to not change the date format to
  #   ISO.  This disables one of Sequel's optimizations.
  #
  # Changes in these settings only affect future connections.  To make
  # sure that they are applied, they should generally be called right
  # after the Database object is instantiated and before a connection
  # is actually made. For example, to use whatever the server defaults are:
  #
  #   DB = Sequel.postgres(...)
  #   Sequel::Postgres.client_min_messages = nil
  #   Sequel::Postgres.force_standard_strings = false
  #   Sequel::Postgres.use_iso_date_format = false
  #   # A connection to the server is not made until here
  #   DB[:t].all
  #
  # The reason they can't be done earlier is that the Sequel::Postgres
  # module is not loaded until a Database object which uses PostgreSQL
  # is created.
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
      # Add an exclusion constraint when creating the table. elements should be
      # an array of 2 element arrays, with the first element being the column or
      # expression the exclusion constraint is applied to, and the second element
      # being the operator to use for the column/expression to check for exclusion.
      #
      # Example:
      #
      #   exclusion_constraint([[:col1, '&&'], [:col2, '=']])
      #   # EXCLUDE USING gist (col1 WITH &&, col2 WITH =)
      #
      # Options supported:
      #
      # :name :: Name the constraint with the given name (useful if you may
      #          need to drop the constraint later)
      # :using :: Override the index_method for the exclusion constraint (defaults to gist).
      # :where :: Create a partial exclusion constraint, which only affects
      #           a subset of table rows, value should be a filter expression.
      def exclude(elements, opts={})
        constraints << {:type => :exclude, :elements => elements}.merge(opts)
      end
    end

    class AlterTableGenerator < Sequel::Schema::AlterTableGenerator
      # Adds an exclusion constraint to an existing table, see
      # CreateTableGenerator#exclude.
      def add_exclusion_constraint(elements, opts={})
        @operations << {:op => :add_constraint, :type => :exclude, :elements => elements}.merge(opts)
      end

      # Validate the constraint with the given name, which should have
      # been added previously with NOT VALID.
      def validate_constraint(name)
        @operations << {:op => :validate_constraint, :name => name}
      end
    end

    # Methods shared by Database instances that connect to PostgreSQL.
    module DatabaseMethods
      EXCLUDE_SCHEMAS = /pg_*|information_schema/i
      PREPARED_ARG_PLACEHOLDER = LiteralString.new('$').freeze
      RE_CURRVAL_ERROR = /currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/.freeze
      SYSTEM_TABLE_REGEXP = /^pg|sql/.freeze
      FOREIGN_KEY_LIST_ON_DELETE_MAP = {'a'.freeze=>:no_action, 'r'.freeze=>:restrict, 'c'.freeze=>:cascade, 'n'.freeze=>:set_null, 'd'.freeze=>:set_default}.freeze

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
          pg_namespace name, pg_constraint cons
        WHERE seq.oid = dep.objid
          AND seq.relnamespace  = name.oid
          AND seq.relkind = 'S'
          AND attr.attrelid = dep.refobjid
          AND attr.attnum = dep.refobjsubid
          AND attr.attrelid = cons.conrelid
          AND attr.attnum = cons.conkey[1]
          AND cons.contype = 'p'
      end_sql
      ).strip.gsub(/\s+/, ' ').freeze

      # Commit an existing prepared transaction with the given transaction
      # identifier string.
      def commit_prepared_transaction(transaction_id)
        run("COMMIT PREPARED #{literal(transaction_id)}")
      end

      # Creates the function in the database.  Arguments:
      # * name : name of the function to create
      # * definition : string definition of the function, or object file for a dynamically loaded C function.
      # * opts : options hash:
      #   * :args : function arguments, can be either a symbol or string specifying a type or an array of 1-3 elements:
      #     * element 1 : argument data type
      #     * element 2 : argument name
      #     * element 3 : argument mode (e.g. in, out, inout)
      #   * :behavior : Should be IMMUTABLE, STABLE, or VOLATILE.  PostgreSQL assumes VOLATILE by default.
      #   * :cost : The estimated cost of the function, used by the query planner.
      #   * :language : The language the function uses.  SQL is the default.
      #   * :link_symbol : For a dynamically loaded see function, the function's link symbol if different from the definition argument.
      #   * :returns : The data type returned by the function.  If you are using OUT or INOUT argument modes, this is ignored.
      #     Otherwise, if this is not specified, void is used by default to specify the function is not supposed to return a value.
      #   * :rows : The estimated number of rows the function will return.  Only use if the function returns SETOF something.
      #   * :security_definer : Makes the privileges of the function the same as the privileges of the user who defined the function instead of
      #     the privileges of the user who runs the function.  There are security implications when doing this, see the PostgreSQL documentation.
      #   * :set : Configuration variables to set while the function is being run, can be a hash or an array of two pairs.  search_path is
      #     often used here if :security_definer is used.
      #   * :strict : Makes the function return NULL when any argument is NULL.
      def create_function(name, definition, opts={})
        self << create_function_sql(name, definition, opts)
      end

      # Create the procedural language in the database. Arguments:
      # * name : Name of the procedural language (e.g. plpgsql)
      # * opts : options hash:
      #   * :handler : The name of a previously registered function used as a call handler for this language.
      #   * :replace: Replace the installed language if it already exists (on PostgreSQL 9.0+).
      #   * :trusted : Marks the language being created as trusted, allowing unprivileged users to create functions using this language.
      #   * :validator : The name of previously registered function used as a validator of functions defined in this language.
      def create_language(name, opts={})
        self << create_language_sql(name, opts)
      end

      # Create a schema in the database. Arguments:
      # * name : Name of the schema (e.g. admin)
      def create_schema(name)
        self << create_schema_sql(name)
      end

      # Create a trigger in the database.  Arguments:
      # * table : the table on which this trigger operates
      # * name : the name of this trigger
      # * function : the function to call for this trigger, which should return type trigger.
      # * opts : options hash:
      #   * :after : Calls the trigger after execution instead of before.
      #   * :args : An argument or array of arguments to pass to the function.
      #   * :each_row : Calls the trigger for each row instead of for each statement.
      #   * :events : Can be :insert, :update, :delete, or an array of any of those. Calls the trigger whenever that type of statement is used.  By default,
      #     the trigger is called for insert, update, or delete.
      def create_trigger(table, name, function, opts={})
        self << create_trigger_sql(table, name, function, opts)
      end

      # PostgreSQL uses the :postgres database type.
      def database_type
        :postgres
      end

      # Drops the function from the database. Arguments:
      # * name : name of the function to drop
      # * opts : options hash:
      #   * :args : The arguments for the function.  See create_function_sql.
      #   * :cascade : Drop other objects depending on this function.
      #   * :if_exists : Don't raise an error if the function doesn't exist.
      def drop_function(name, opts={})
        self << drop_function_sql(name, opts)
      end

      # Drops a procedural language from the database.  Arguments:
      # * name : name of the procedural language to drop
      # * opts : options hash:
      #   * :cascade : Drop other objects depending on this function.
      #   * :if_exists : Don't raise an error if the function doesn't exist.
      def drop_language(name, opts={})
        self << drop_language_sql(name, opts)
      end

      # Drops a schema from the database.  Arguments:
      # * name : name of the schema to drop
      # * opts : options hash:
      #   * :cascade : Drop all objects in this schema.
      #   * :if_exists : Don't raise an error if the schema doesn't exist.
      def drop_schema(name, opts={})
        self << drop_schema_sql(name, opts)
      end

      # Drops a trigger from the database.  Arguments:
      # * table : table from which to drop the trigger
      # * name : name of the trigger to drop
      # * opts : options hash:
      #   * :cascade : Drop other objects depending on this function.
      #   * :if_exists : Don't raise an error if the function doesn't exist.
      def drop_trigger(table, name, opts={})
        self << drop_trigger_sql(table, name, opts)
      end

      # Return full foreign key information using the pg system tables, including
      # :name, :on_delete, :on_update, and :deferrable entries in the hashes.
      def foreign_key_list(table, opts={})
        m = output_identifier_meth
        im = input_identifier_meth
        schema, table = schema_and_table(table)
        range = 0...32

        base_ds = metadata_dataset.
          where(:cl__relkind=>'r', :co__contype=>'f', :cl__relname=>im.call(table)).
          from(:pg_constraint___co).
          join(:pg_class___cl, :oid=>:conrelid)

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
          ds = ds.join(:pg_namespace___nsp, :oid=>:cl__relnamespace).
            where(:nsp__nspname=>im.call(schema))
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
      def indexes(table, opts={})
        m = output_identifier_meth
        im = input_identifier_meth
        schema, table = schema_and_table(table)
        range = 0...32
        attnums = server_version >= 80100 ? SQL::Function.new(:ANY, :ind__indkey) : range.map{|x| SQL::Subscript.new(:ind__indkey, [x])}
        ds = metadata_dataset.
          from(:pg_class___tab).
          join(:pg_index___ind, :indrelid=>:oid, im.call(table)=>:relname).
          join(:pg_class___indc, :oid=>:indexrelid).
          join(:pg_attribute___att, :attrelid=>:tab__oid, :attnum=>attnums).
          filter(:indc__relkind=>'i', :ind__indisprimary=>false, :indexprs=>nil, :indpred=>nil, :indisvalid=>true).
          order(:indc__relname, SQL::CaseExpression.new(range.map{|x| [SQL::Subscript.new(:ind__indkey, [x]), x]}, 32, :att__attnum)).
          select(:indc__relname___name, :ind__indisunique___unique, :att__attname___column)

        ds.join!(:pg_namespace___nsp, :oid=>:tab__relnamespace, :nspname=>schema.to_s) if schema
        ds.filter!(:indisready=>true, :indcheckxmin=>false) if server_version >= 80300

        indexes = {}
        ds.each do |r|
          i = indexes[m.call(r[:name])] ||= {:columns=>[], :unique=>r[:unique]}
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
      def notify(channel, opts={})
        execute_ddl("NOTIFY #{channel}#{", #{literal(opts[:payload].to_s)}" if opts[:payload]}", opts)
      end

      # Return primary key for the given table.
      def primary_key(table, opts={})
        quoted_table = quote_schema_table(table)
        @primary_keys.fetch(quoted_table) do
          schema, table = schema_and_table(table)
          sql = "#{SELECT_PK_SQL} AND pg_class.relname = #{literal(table)}"
          sql << "AND pg_namespace.nspname = #{literal(schema)}" if schema
          @primary_keys[quoted_table] = fetch(sql).single_value
        end
      end

      # Return the sequence providing the default for the primary key for the given table.
      def primary_key_sequence(table, opts={})
        quoted_table = quote_schema_table(table)
        @primary_key_sequences.fetch(quoted_table) do
          schema, table = schema_and_table(table)
          table = literal(table)
          sql = "#{SELECT_SERIAL_SEQUENCE_SQL} AND seq.relname = #{table}"
          sql << " AND name.nspname = #{literal(schema)}" if schema
          unless pks = fetch(sql).single_record
            sql = "#{SELECT_CUSTOM_SEQUENCE_SQL} AND t.relname = #{table}"
            sql << " AND name.nspname = #{literal(schema)}" if schema
            pks = fetch(sql).single_record
          end

          @primary_key_sequences[quoted_table] = if pks
            literal(SQL::QualifiedIdentifier.new(pks[:schema], LiteralString.new(pks[:sequence])))
          end
        end
      end

      # Reset the primary key sequence for the given table, baseing it on the
      # maximum current value of the table's primary key.
      def reset_primary_key_sequence(table)
        return unless seq = primary_key_sequence(table)
        pk = SQL::Identifier.new(primary_key(table))
        db = self
        seq_ds = db.from(LiteralString.new(seq))
        get{setval(seq, db[table].select{coalesce(max(pk)+seq_ds.select{:increment_by}, seq_ds.select(:min_value))}, false)}
      end

      # Rollback an existing prepared transaction with the given transaction
      # identifier string.
      def rollback_prepared_transaction(transaction_id)
        run("ROLLBACK PREPARED #{literal(transaction_id)}")
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

      # PostgreSQL supports DROP TABLE IF EXISTS
      def supports_drop_table_if_exists?
        true
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
      # * :schema - The schema to search (default_schema by default)
      # * :server - The server to use
      def tables(opts={}, &block)
        pg_class_relname('r', opts, &block)
      end

      # Check whether the given type name string/symbol (e.g. :hstore) is supported by
      # the database.
      def type_supported?(type)
        @supported_types ||= {}
        @supported_types.fetch(type){@supported_types[type] = (from(:pg_type).filter(:typtype=>'b', :typname=>type.to_s).count > 0)}
      end

      # Array of symbols specifying view names in the current database.
      #
      # Options:
      # * :schema - The schema to search (default_schema by default)
      # * :server - The server to use
      def views(opts={})
        pg_class_relname('v', opts)
      end

      private

      # Use a PostgreSQL-specific alter table generator
      def alter_table_generator_class
        Postgres::AlterTableGenerator
      end

      # Handle :using option for set_column_type op, and the :validate_constraint op.
      def alter_table_sql(table, op)
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
          "ALTER TABLE #{quote_schema_table(table)} VALIDATE CONSTRAINT #{quote_identifier(op[:name])}"
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

      # If the :prepare option is given and we aren't in a savepoint,
      # prepare the transaction for a two-phase commit.
      def commit_transaction(conn, opts={})
        if (s = opts[:prepare]) && _trans(conn)[:savepoint_level] <= 1
          log_connection_execute(conn, "PREPARE TRANSACTION #{literal(s)}")
        else
          super
        end
      end

      # The SQL queries to execute when starting a new connection.
      def connection_configuration_sqls
        sqls = []
        sqls << "SET standard_conforming_strings = ON" if Postgres.force_standard_strings
        if cmm = Postgres.client_min_messages
          sqls << "SET client_min_messages = '#{cmm.to_s.upcase}'"
        end
        sqls
      end

      # Handle exclusion constraints.
      def constraint_definition_sql(constraint)
        case constraint[:type]
        when :exclude
          elements = constraint[:elements].map{|c, op| "#{literal(c)} WITH #{op}"}.join(', ')
          "#{"CONSTRAINT #{quote_identifier(constraint[:name])} " if constraint[:name]}EXCLUDE USING #{constraint[:using]||'gist'} (#{elements})#{" WHERE #{filter_expr(constraint[:where])}" if constraint[:where]}"
        when :foreign_key
          sql = super
          if constraint[:not_valid]
            sql << " NOT VALID"
          end
          sql
        else
          super
        end
      end

      # SQL statement to create database function.
      def create_function_sql(name, definition, opts={})
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
      def create_language_sql(name, opts={})
        "CREATE#{' OR REPLACE' if opts[:replace] && server_version >= 90000}#{' TRUSTED' if opts[:trusted]} LANGUAGE #{name}#{" HANDLER #{opts[:handler]}" if opts[:handler]}#{" VALIDATOR #{opts[:validator]}" if opts[:validator]}"
      end

      # SQL for creating a schema.
      def create_schema_sql(name)
        "CREATE SCHEMA #{quote_identifier(name)}"
      end

      # Use a PostgreSQL-specific create table generator
      def create_table_generator_class
        Postgres::CreateTableGenerator
      end

      # SQL for creating a database trigger.
      def create_trigger_sql(table, name, function, opts={})
        events = opts[:events] ? Array(opts[:events]) : [:insert, :update, :delete]
        whence = opts[:after] ? 'AFTER' : 'BEFORE'
        "CREATE TRIGGER #{name} #{whence} #{events.map{|e| e.to_s.upcase}.join(' OR ')} ON #{quote_schema_table(table)}#{' FOR EACH ROW' if opts[:each_row]} EXECUTE PROCEDURE #{function}(#{Array(opts[:args]).map{|a| literal(a)}.join(', ')})"
      end

      # The errors that the main adapters can raise, depends on the adapter being used
      def database_error_classes
        CONVERTED_EXCEPTIONS
      end

      # SQL for dropping a function from the database.
      def drop_function_sql(name, opts={})
        "DROP FUNCTION#{' IF EXISTS' if opts[:if_exists]} #{name}#{sql_function_args(opts[:args])}#{' CASCADE' if opts[:cascade]}"
      end

      # Support :if_exists, :cascade, and :concurrently options.
      def drop_index_sql(table, op)
        "DROP INDEX#{' CONCURRENTLY' if op[:concurrently]}#{' IF EXISTS' if op[:if_exists]} #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))}#{' CASCADE' if op[:cascade]}"
      end

      # SQL for dropping a procedural language from the database.
      def drop_language_sql(name, opts={})
        "DROP LANGUAGE#{' IF EXISTS' if opts[:if_exists]} #{name}#{' CASCADE' if opts[:cascade]}"
      end

      # SQL for dropping a schema from the database.
      def drop_schema_sql(name, opts={})
        "DROP SCHEMA#{' IF EXISTS' if opts[:if_exists]} #{quote_identifier(name)}#{' CASCADE' if opts[:cascade]}"
      end

      # SQL for dropping a trigger from the database.
      def drop_trigger_sql(table, name, opts={})
        "DROP TRIGGER#{' IF EXISTS' if opts[:if_exists]} #{name} ON #{quote_schema_table(table)}#{' CASCADE' if opts[:cascade]}"
      end

      # If opts includes a :schema option, or a default schema is used, restrict the dataset to
      # that schema.  Otherwise, just exclude the default PostgreSQL schemas except for public.
      def filter_schema(ds, opts)
        if schema = opts[:schema] || default_schema
          ds.filter(:pg_namespace__nspname=>schema.to_s)
        else
          ds.exclude(:pg_namespace__nspname=>EXCLUDE_SCHEMAS)
        end
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
          index_type = :gin
        when :spatial
          index_type = :gist
        end
        "CREATE #{unique}INDEX#{' CONCURRENTLY' if index[:concurrently]} #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{"USING #{index_type} " if index_type}#{expr}#{filter}"
      end

      # Backbone of the tables and views support.
      def pg_class_relname(type, opts)
        ds = metadata_dataset.from(:pg_class).filter(:relkind=>type).select(:relname).exclude(SQL::StringExpression.like(:relname, SYSTEM_TABLE_REGEXP)).server(opts[:server]).join(:pg_namespace, :oid=>:relnamespace)
        ds = filter_schema(ds, opts)
        m = output_identifier_meth
        block_given? ? yield(ds) : ds.map{|r| m.call(r[:relname])}
      end

      # Use a dollar sign instead of question mark for the argument
      # placeholder.
      def prepared_arg_placeholder
        PREPARED_ARG_PLACEHOLDER
      end

      # Remove the cached entries for primary keys and sequences when a table is
      # changed.
      def remove_cached_schema(table)
        tab = quote_schema_table(table)
        @primary_keys.delete(tab)
        @primary_key_sequences.delete(tab)
        super
      end

      # SQL DDL statement for renaming a table. PostgreSQL doesn't allow you to change a table's schema in
      # a rename table operation, so speciying a new schema in new_name will not have an effect.
      def rename_table_sql(name, new_name)
        "ALTER TABLE #{quote_schema_table(name)} RENAME TO #{quote_identifier(schema_and_table(new_name).last)}"
      end

      # PostgreSQL's autoincrementing primary keys are of type integer or bigint
      # using a nextval function call as a default.
      def schema_autoincrementing_primary_key?(schema)
        super && schema[:default] =~ /\Anextval/io
      end

      # The dataset used for parsing table schemas, using the pg_* system catalogs.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])
        m2 = input_identifier_meth(opts[:dataset])
        ds = metadata_dataset.select(:pg_attribute__attname___name,
            SQL::Function.new(:format_type, :pg_type__oid, :pg_attribute__atttypmod).as(:db_type),
            SQL::Function.new(:pg_get_expr, :pg_attrdef__adbin, :pg_class__oid).as(:default),
            SQL::BooleanExpression.new(:NOT, :pg_attribute__attnotnull).as(:allow_null),
            SQL::Function.new(:COALESCE, SQL::BooleanExpression.from_value_pairs(:pg_attribute__attnum => SQL::Function.new(:ANY, :pg_index__indkey)), false).as(:primary_key),
            :pg_namespace__nspname).
          from(:pg_class).
          join(:pg_attribute, :attrelid=>:oid).
          join(:pg_type, :oid=>:atttypid).
          join(:pg_namespace, :oid=>:pg_class__relnamespace).
          left_outer_join(:pg_attrdef, :adrelid=>:pg_class__oid, :adnum=>:pg_attribute__attnum).
          left_outer_join(:pg_index, :indrelid=>:pg_class__oid, :indisprimary=>true).
          filter(:pg_attribute__attisdropped=>false).
          filter{|o| o.pg_attribute__attnum > 0}.
          filter(:pg_class__relname=>m2.call(table_name)).
          order(:pg_attribute__attnum)
        ds = filter_schema(ds, opts)
        current_schema = nil
        ds.map do |row|
          sch = row.delete(:nspname)
          if current_schema
            if sch != current_schema
              raise Error, "columns from tables in two separate schema were returned (please specify a schema): #{current_schema.inspect}, #{sch.inspect}"
            end
          else
            current_schema = sch
          end
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
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
    end

    # Instance methods for datasets that connect to a PostgreSQL database.
    module DatasetMethods
      ACCESS_SHARE = 'ACCESS SHARE'.freeze
      ACCESS_EXCLUSIVE = 'ACCESS EXCLUSIVE'.freeze
      BOOL_FALSE = 'false'.freeze
      BOOL_TRUE = 'true'.freeze
      COMMA_SEPARATOR = ', '.freeze
      DELETE_CLAUSE_METHODS = Dataset.clause_methods(:delete, %w'delete from using where returning')
      DELETE_CLAUSE_METHODS_91 = Dataset.clause_methods(:delete, %w'with delete from using where returning')
      EXCLUSIVE = 'EXCLUSIVE'.freeze
      EXPLAIN = 'EXPLAIN '.freeze
      EXPLAIN_ANALYZE = 'EXPLAIN ANALYZE '.freeze
      FOR_SHARE = ' FOR SHARE'.freeze
      INSERT_CLAUSE_METHODS = Dataset.clause_methods(:insert, %w'insert into columns values returning')
      INSERT_CLAUSE_METHODS_91 = Dataset.clause_methods(:insert, %w'with insert into columns values returning')
      LOCK = 'LOCK TABLE %s IN %s MODE'.freeze
      NULL = LiteralString.new('NULL').freeze
      PG_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
      ROW_EXCLUSIVE = 'ROW EXCLUSIVE'.freeze
      ROW_SHARE = 'ROW SHARE'.freeze
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'select distinct columns from join where group having compounds order limit lock')
      SELECT_CLAUSE_METHODS_84 = Dataset.clause_methods(:select, %w'with select distinct columns from join where group having window compounds order limit lock')
      SHARE = 'SHARE'.freeze
      SHARE_ROW_EXCLUSIVE = 'SHARE ROW EXCLUSIVE'.freeze
      SHARE_UPDATE_EXCLUSIVE = 'SHARE UPDATE EXCLUSIVE'.freeze
      SQL_WITH_RECURSIVE = "WITH RECURSIVE ".freeze
      UPDATE_CLAUSE_METHODS = Dataset.clause_methods(:update, %w'update table set from where returning')
      UPDATE_CLAUSE_METHODS_91 = Dataset.clause_methods(:update, %w'with update table set from where returning')
      SPACE = Dataset::SPACE
      FROM = Dataset::FROM
      APOS = Dataset::APOS
      APOS_RE = Dataset::APOS_RE
      DOUBLE_APOS = Dataset::DOUBLE_APOS
      PAREN_OPEN = Dataset::PAREN_OPEN
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      COMMA = Dataset::COMMA
      AS = Dataset::AS
      XOR_OP = ' # '.freeze
      CRLF = "\r\n".freeze
      BLOB_RE = /[\000-\037\047\134\177-\377]/n.freeze
      WINDOW = " WINDOW ".freeze
      EMPTY_STRING = ''.freeze

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
      # PostgreSQL xor operator (#).
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
        else
          super
        end
      end

      # Return the results of an EXPLAIN query as a string
      def explain(opts={})
        with_sql((opts[:analyze] ? EXPLAIN_ANALYZE : EXPLAIN) + select_sql).map(QUERY_PLAN).join(CRLF)
      end

      # Return a cloned dataset which will use FOR SHARE to lock returned rows.
      def for_share
        lock_style(:share)
      end

      # PostgreSQL specific full text search syntax, using tsearch2 (included
      # in 8.3 by default, and available for earlier versions as an add-on).
      def full_text_search(cols, terms, opts = {})
        lang = opts[:language] || 'simple'
        terms = terms.join(' | ') if terms.is_a?(Array)
        filter("to_tsvector(?::regconfig, ?) @@ to_tsquery(?::regconfig, ?)", lang, full_text_string_join(cols), lang, terms)
      end

      # Insert given values into the database.
      def insert(*values)
        if @opts[:returning]
          # already know which columns to return, let the standard code
          # handle it
          super
        elsif @opts[:sql]
          # raw SQL used, so don't know which table is being inserted
          # into, and therefore can't determine primary key.  Run the
          # insert statement and return nil.
          super
          nil
        else
          # Force the use of RETURNING with the primary key value.
          returning(insert_pk).insert(*values){|r| return r.values.first}
        end
      end

      # Insert a record returning the record inserted
      def insert_select(*values)
        returning.insert(*values){|r| return r}
      end

      # Locks all tables in the dataset's FROM clause (but not in JOINs) with
      # the specified mode (e.g. 'EXCLUSIVE').  If a block is given, starts
      # a new transaction, locks the table, and yields.  If a block is not given
      # just locks the tables.  Note that PostgreSQL will probably raise an error
      # if you lock the table outside of an existing transaction.  Returns nil.
      def lock(mode, opts={})
        if block_given? # perform locking inside a transaction and yield to block
          @db.transaction(opts){lock(mode, opts); yield}
        else
          @db.execute(LOCK % [source_list(@opts[:from]), mode], opts) # lock without a transaction
        end
        nil
      end

      # PostgreSQL allows inserting multiple rows at once.
      def multi_insert_sql(columns, values)
        sql = LiteralString.new('VALUES ')
        expression_list_append(sql, values.map{|r| Array(r)})
        [insert_sql(columns, sql)]
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

      # PostgreSQL supports modifying joined datasets
      def supports_modifying_joins?
        true
      end

      # Returning is always supported.
      def supports_returning?(type)
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
      #   tables with Foreign Keys.
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
      def truncate(opts = {})
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
      def _import(columns, values, opts={})
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

      # PostgreSQL allows deleting from joined datasets
      def delete_clause_methods
        if server_version >= 90100
          DELETE_CLAUSE_METHODS_91
        else
          DELETE_CLAUSE_METHODS
        end
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

      # PostgreSQL allows a RETURNING clause.
      def insert_clause_methods
        if server_version >= 90100
          INSERT_CLAUSE_METHODS_91
        else
          INSERT_CLAUSE_METHODS
        end
      end

      # Return the primary key to use for RETURNING in an INSERT statement
      def insert_pk
        if (f = opts[:from]) && !f.empty? && (pk = db.primary_key(f.first))
          Sequel::SQL::Identifier.new(pk)
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

      # The order of clauses in the SELECT SQL statement
      def select_clause_methods
        server_version >= 80400 ? SELECT_CLAUSE_METHODS_84 : SELECT_CLAUSE_METHODS
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

      # Concatenate the expressions with a space in between
      def full_text_string_join(cols)
        cols = Array(cols).map{|x| SQL::Function.new(:COALESCE, x, EMPTY_STRING)}
        cols = cols.zip([SPACE] * cols.length).flatten
        cols.pop
        SQL::StringExpression.new(:'||', *cols)
      end

      # PostgreSQL splits the main table from the joined tables
      def update_clause_methods
        if server_version >= 90100
          UPDATE_CLAUSE_METHODS_91
        else
          UPDATE_CLAUSE_METHODS
        end
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
