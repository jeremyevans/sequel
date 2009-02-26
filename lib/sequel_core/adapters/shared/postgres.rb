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
    
    # By default, Sequel sets the minimum level of log messages sent to the client
    # to WARNING, where PostgreSQL uses a default of NOTICE.  This is to avoid a lot
    # of mostly useless messages when running migrations, such as a couple of lines
    # for every serial primary key field.
    metaattr_accessor :client_min_messages

    # By default, Sequel forces the use of standard strings, so that
    # '\\' is interpreted as \\ and not \.  While PostgreSQL defaults
    # to interpreting plain strings as extended strings, this will change
    # in a future version of PostgreSQL.  Sequel assumes that SQL standard
    # strings will be used.
    metaattr_accessor :force_standard_strings

    # Methods shared by adapter/connection instances.
    module AdapterMethods
      attr_writer :db
      
      SELECT_CURRVAL = "SELECT currval('%s')".freeze
      SELECT_CUSTOM_SEQUENCE = proc do |schema, table| <<-end_sql
        SELECT '"' || name.nspname || '"."' || CASE  
            WHEN split_part(def.adsrc, '''', 2) ~ '.' THEN  
              substr(split_part(def.adsrc, '''', 2),  
                     strpos(split_part(def.adsrc, '''', 2), '.')+1) 
            ELSE split_part(def.adsrc, '''', 2)  
          END || '"'
        FROM pg_class t
        JOIN pg_namespace  name ON (t.relnamespace = name.oid)
        JOIN pg_attribute  attr ON (t.oid = attrelid)
        JOIN pg_attrdef    def  ON (adrelid = attrelid AND adnum = attnum)
        JOIN pg_constraint cons ON (conrelid = adrelid AND adnum = conkey[1])
        WHERE cons.contype = 'p'
          AND def.adsrc ~* 'nextval'
          #{"AND name.nspname = '#{schema}'" if schema}
          AND t.relname = '#{table}'
      end_sql
      end
      SELECT_PK = proc do |schema, table| <<-end_sql
        SELECT pg_attribute.attname
        FROM pg_class, pg_attribute, pg_index, pg_namespace
        WHERE pg_class.oid = pg_attribute.attrelid
          AND pg_class.relnamespace  = pg_namespace.oid
          AND pg_class.oid = pg_index.indrelid
          AND pg_index.indkey[0] = pg_attribute.attnum
          AND pg_index.indisprimary = 't'
          #{"AND pg_namespace.nspname = '#{schema}'" if schema}
          AND pg_class.relname = '#{table}'
      end_sql
      end
      SELECT_SERIAL_SEQUENCE = proc do |schema, table| <<-end_sql
        SELECT  '"' || name.nspname || '"."' || seq.relname || '"'
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
          #{"AND name.nspname = '#{schema}'" if schema}
          AND seq.relname = '#{table}'
      end_sql
      end
      
      # Depth of the current transaction on this connection, used
      # to implement multi-level transactions with savepoints.
      attr_accessor :transaction_depth
      
      # Apply connection settings for this connection. Currently, turns
      # standard_conforming_strings ON if Postgres.force_standard_strings
      # is true.
      def apply_connection_settings
        if Postgres.force_standard_strings
          sql = "SET standard_conforming_strings = ON"
          @db.log_info(sql)
          # This setting will only work on PostgreSQL 8.2 or greater
          # and we don't know the server version at this point, so
          # try it unconditionally and rescue any errors.
          execute(sql) rescue nil
        end
        if cmm = Postgres.client_min_messages
          sql = "SET client_min_messages = '#{cmm.to_s.upcase}'"
          @db.log_info(sql)
          execute(sql)
        end
      end

      # Get the last inserted value for the given sequence.
      def last_insert_id(sequence)
        sql = SELECT_CURRVAL % sequence
        @db.log_info(sql)
        execute(sql) do |r|
          val = single_value(r)
          return val.to_i if val
        end
      end
      
      # Get the primary key for the given table.
      def primary_key(schema, table)
        sql = SELECT_PK[schema, table]
        @db.log_info(sql)
        execute(sql) do |r|
          return single_value(r)
        end
      end
      
      # Get the primary key and sequence for the given table.
      def sequence(schema, table)
        sql = SELECT_SERIAL_SEQUENCE[schema, table]
        @db.log_info(sql)
        execute(sql) do |r|
          seq = single_value(r)
          return seq if seq
        end
        
        sql = SELECT_CUSTOM_SEQUENCE[schema, table]
        @db.log_info(sql)
        execute(sql) do |r|
          return single_value(r)
        end
      end
    end
    
    # Methods shared by Database instances that connect to PostgreSQL.
    module DatabaseMethods
      PREPARED_ARG_PLACEHOLDER = LiteralString.new('$').freeze
      RE_CURRVAL_ERROR = /currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/.freeze
      SQL_BEGIN = 'BEGIN'.freeze
      SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
      SQL_COMMIT = 'COMMIT'.freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze
      SQL_ROLLBACK = 'ROLLBACK'.freeze
      SQL_RELEASE_SAVEPOINT = 'RELEASE SAVEPOINT autopoint_%d'.freeze
      SYSTEM_TABLE_REGEXP = /^pg|sql/.freeze
      TYPES = Sequel::Schema::SQL::TYPES.merge(File=>'bytea', String=>'text')

      # Creates the function in the database.  See create_function_sql for arguments.
      def create_function(*args)
        self << create_function_sql(*args)
      end
      
      # SQL statement to create database function. Arguments:
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
      
      # Create the procedural language in the database.  See create_language_sql for arguments.
      def create_language(*args)
        self << create_language_sql(*args)
      end
      
      # SQL for creating a procedural language. Arguments:
      # * name : Name of the procedural language (e.g. plpgsql)
      # * opts : options hash:
      #   * :handler : The name of a previously registered function used as a call handler for this language.
      #   * :trusted : Marks the language being created as trusted, allowing unprivileged users to create functions using this language.
      #   * :validator : The name of previously registered function used as a validator of functions defined in this language.
      def create_language_sql(name, opts={})
        "CREATE#{' TRUSTED' if opts[:trusted]} LANGUAGE #{name}#{" HANDLER #{opts[:handler]}" if opts[:handler]}#{" VALIDATOR #{opts[:validator]}" if opts[:validator]}"
      end
      
      # Create a trigger in the database.  See create_trigger_sql for arguments.
      def create_trigger(*args)
        self << create_trigger_sql(*args)
      end
      
      # SQL for creating a database trigger. Arguments:
      # * table : the table on which this trigger operates
      # * name : the name of this trigger
      # * function : the function to call for this trigger, which should return type trigger.
      # * opts : options hash:
      #   * :after : Calls the trigger after execution instead of before.
      #   * :args : An argument or array of arguments to pass to the function.
      #   * :each_row : Calls the trigger for each row instead of for each statement.
      #   * :events : Can be :insert, :update, :delete, or an array of any of those. Calls the trigger whenever that type of statement is used.  By default,
      #     the trigger is called for insert, update, or delete.
      def create_trigger_sql(table, name, function, opts={})
        events = opts[:events] ? Array(opts[:events]) : [:insert, :update, :delete]
        whence = opts[:after] ? 'AFTER' : 'BEFORE'
        "CREATE TRIGGER #{name} #{whence} #{events.map{|e| e.to_s.upcase}.join(' OR ')} ON #{quote_schema_table(table)}#{' FOR EACH ROW' if opts[:each_row]} EXECUTE PROCEDURE #{function}(#{Array(opts[:args]).map{|a| literal(a)}.join(', ')})"
      end
      
      # Drops the function from the database.  See drop_function_sql for arguments.
      def drop_function(*args)
        self << drop_function_sql(*args)
      end
      
      # SQL for dropping a function from the database.  Arguments:
      # * name : name of the function to drop
      # * opts : options hash:
      #   * :args : The arguments for the function.  See create_function_sql.
      #   * :cascade : Drop other objects depending on this function.
      #   * :if_exists : Don't raise an error if the function doesn't exist.
      def drop_function_sql(name, opts={})
        "DROP FUNCTION#{' IF EXISTS' if opts[:if_exists]} #{name}#{sql_function_args(opts[:args])}#{' CASCADE' if opts[:cascade]}"
      end
      
      # Drops a procedural language from the database.  See drop_language_sql for arguments.
      def drop_language(*args)
        self << drop_language_sql(*args)
      end
      
      # SQL for dropping a procedural language from the database.  Arguments:
      # * name : name of the procedural language to drop
      # * opts : options hash:
      #   * :cascade : Drop other objects depending on this function.
      #   * :if_exists : Don't raise an error if the function doesn't exist.
      def drop_language_sql(name, opts={})
        "DROP LANGUAGE#{' IF EXISTS' if opts[:if_exists]} #{name}#{' CASCADE' if opts[:cascade]}"
      end

      # Remove the cached entries for primary keys and sequences when dropping a table.
      def drop_table(*names)
        names.each do |name|
          name = quote_schema_table(name)
          @primary_keys.delete(name)
          @primary_key_sequences.delete(name)
        end
        super
      end

      # Always CASCADE the table drop
      def drop_table_sql(name)
        "DROP TABLE #{quote_schema_table(name)} CASCADE"
      end
      
      # Drops a trigger from the database.  See drop_trigger_sql for arguments.
      def drop_trigger(*args)
        self << drop_trigger_sql(*args)
      end
      
      # SQL for dropping a trigger from the database.  Arguments:
      # * table : table from which to drop the trigger
      # * name : name of the trigger to drop
      # * opts : options hash:
      #   * :cascade : Drop other objects depending on this function.
      #   * :if_exists : Don't raise an error if the function doesn't exist.
      def drop_trigger_sql(table, name, opts={})
        "DROP TRIGGER#{' IF EXISTS' if opts[:if_exists]} #{name} ON #{quote_schema_table(table)}#{' CASCADE' if opts[:cascade]}"
      end

      # PostgreSQL specific index SQL.
      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        expr = literal(Array(index[:columns]))
        unique = "UNIQUE " if index[:unique]
        index_type = index[:type]
        filter = index[:where] || index[:filter]
        filter = " WHERE #{filter_expr(filter)}" if filter
        case index_type
        when :full_text
          cols = Array(index[:columns]).map{|x| SQL::Function.new(:COALESCE, x, '')}.sql_string_join(' ')
          expr = "(to_tsvector(#{literal(index[:language] || 'simple')}, #{literal(cols)}))"
          index_type = :gin
        when :spatial
          index_type = :gist
        end
        "CREATE #{unique}INDEX #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{"USING #{index_type} " if index_type}#{expr}#{filter}"
      end
      
      # Dataset containing all current database locks 
      def locks
        dataset.from(:pg_class).join(:pg_locks, :relation=>:relfilenode).select(:pg_class__relname, Sequel::SQL::ColumnAll.new(:pg_locks))
      end
      
      # Return primary key for the given table.
      def primary_key(table, opts={})
        quoted_table = quote_schema_table(table)
        return @primary_keys[quoted_table] if @primary_keys.include?(quoted_table)
        @primary_keys[quoted_table] = if conn = opts[:conn]
          conn.primary_key(*schema_and_table(table))
        else
          synchronize(opts[:server]){|con| con.primary_key(*schema_and_table(table))}
        end
      end
      
      # Return the sequence providing the default for the primary key for the given table.
      def primary_key_sequence(table, opts={})
        quoted_table = quote_schema_table(table)
        return @primary_key_sequences[quoted_table] if @primary_key_sequences.include?(quoted_table)
        @primary_key_sequences[quoted_table] = if conn = opts[:conn]
          conn.sequence(*schema_and_table(table))
        else
          synchronize(opts[:server]){|con| con.sequence(*schema_and_table(table))}
        end
      end

      # SQL DDL statement for renaming a table. PostgreSQL doesn't allow you to change a table's schema in
      # a rename table operation, so speciying a new schema in new_name will not have an effect.
      def rename_table_sql(name, new_name)
        "ALTER TABLE #{quote_schema_table(name)} RENAME TO #{quote_identifier(schema_and_table(new_name).last)}"
      end 

      # PostgreSQL uses SERIAL psuedo-type instead of AUTOINCREMENT for
      # managing incrementing primary keys.
      def serial_primary_key_options
        {:primary_key => true, :type => :serial}
      end
      
      # The version of the PostgreSQL server, used for determining capability.
      def server_version(server=nil)
        return @server_version if @server_version
        @server_version = synchronize(server) do |conn|
          (conn.server_version rescue nil) if conn.respond_to?(:server_version)
        end
        unless @server_version
          m = /PostgreSQL (\d+)\.(\d+)\.(\d+)/.match(get(SQL::Function.new(:version)))
          @server_version = (m[1].to_i * 10000) + (m[2].to_i * 100) + m[3].to_i
        end
        @server_version
      end
      
      # Whether the given table exists in the database
      #
      # Options:
      # * :schema - The schema to search (default_schema by default)
      # * :server - The server to use
      def table_exists?(table, opts={})
        schema, table = schema_and_table(table)
        opts[:schema] ||= schema
        tables(opts){|ds| !ds.first(:relname=>ds.send(:input_identifier, table)).nil?}
      end
      
      # Array of symbols specifying table names in the current database.
      # The dataset used is yielded to the block if one is provided,
      # otherwise, an array of symbols of table names is returned.  
      #
      # Options:
      # * :schema - The schema to search (default_schema by default)
      # * :server - The server to use
      def tables(opts={})
        ds = self[:pg_class].filter(:relkind=>'r').select(:relname).exclude(:relname.like(SYSTEM_TABLE_REGEXP)).server(opts[:server])
        ds.join!(:pg_namespace, :oid=>:relnamespace, :nspname=>(opts[:schema]||default_schema).to_s) if opts[:schema] || default_schema
        ds.identifier_input_method = nil
        ds.identifier_output_method = nil
        ds2 = dataset
        block_given? ? yield(ds) : ds.map{|r| ds2.send(:output_identifier, r[:relname])}
      end
      
      # PostgreSQL supports multi-level transactions using save points.
      def transaction(server=nil)
        synchronize(server) do |conn|
          conn.transaction_depth = 0 if conn.transaction_depth.nil?
          if conn.transaction_depth > 0
            log_info(SQL_SAVEPOINT % conn.transaction_depth)
            conn.execute(SQL_SAVEPOINT % conn.transaction_depth)
          else
            log_info(SQL_BEGIN)
            conn.execute(SQL_BEGIN)
          end
          begin
            conn.transaction_depth += 1
            yield conn
          rescue ::Exception => e
            if conn.transaction_depth > 1
              log_info(SQL_ROLLBACK_TO_SAVEPOINT % [conn.transaction_depth - 1])
              conn.execute(SQL_ROLLBACK_TO_SAVEPOINT % [conn.transaction_depth - 1])
            else
              log_info(SQL_ROLLBACK)
              conn.execute(SQL_ROLLBACK) rescue nil
            end
            transaction_error(e, *CONVERTED_EXCEPTIONS)
          ensure
            unless e
              begin
                if conn.transaction_depth < 2
                  log_info(SQL_COMMIT)
                  conn.execute(SQL_COMMIT)
                else
                  log_info(SQL_RELEASE_SAVEPOINT % [conn.transaction_depth - 1])
                  conn.execute(SQL_RELEASE_SAVEPOINT % [conn.transaction_depth - 1])
                end
              rescue => e
                log_info(e.message)
                raise_error(e, :classes=>CONVERTED_EXCEPTIONS)
              end
            end
            conn.transaction_depth -= 1
          end
        end
      end

      private

      # PostgreSQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on input.
      def identifier_input_method_default
        nil
      end
      
      # PostgreSQL folds unquoted identifiers to lowercase, so it shouldn't need to upcase identifiers on output.
      def identifier_output_method_default
        nil
      end

      # The result of the insert for the given table and values.  If values
      # is an array, assume the first column is the primary key and return
      # that.  If values is a hash, lookup the primary key for the table.  If
      # the primary key is present in the hash, return its value.  Otherwise,
      # look up the sequence for the table's primary key.  If one exists,
      # return the last value the of the sequence for the connection.
      def insert_result(conn, table, values)
        case values
        when Hash
          return nil unless pk = primary_key(table, :conn=>conn)
          if pk and pkv = values[pk.to_sym]
            pkv
          else
            begin
              if seq = primary_key_sequence(table, :conn=>conn)
                conn.last_insert_id(seq)
              end
            rescue Exception => e
              raise_error(e, :classes=>CONVERTED_EXCEPTIONS) unless RE_CURRVAL_ERROR.match(e.message)
            end
          end
        when Array
          values.first
        else
          nil
        end
      end
      
      # Use a dollar sign instead of question mark for the argument
      # placeholder.
      def prepared_arg_placeholder
        PREPARED_ARG_PLACEHOLDER
      end
      
      # The dataset used for parsing table schemas, using the pg_* system catalogs.
      def schema_parse_table(table_name, opts)
        ds2 = dataset
        ds = dataset.select(:pg_attribute__attname___name,
            SQL::Function.new(:format_type, :pg_type__oid, :pg_attribute__atttypmod).as(:db_type),
            SQL::Function.new(:pg_get_expr, :pg_attrdef__adbin, :pg_class__oid).as(:default),
            SQL::BooleanExpression.new(:NOT, :pg_attribute__attnotnull).as(:allow_null),
            SQL::Function.new(:COALESCE, {:pg_attribute__attnum => SQL::Function.new(:ANY, :pg_index__indkey)}.sql_expr, false).as(:primary_key)).
          from(:pg_class).
          join(:pg_attribute, :attrelid=>:oid).
          join(:pg_type, :oid=>:atttypid).
          left_outer_join(:pg_attrdef, :adrelid=>:pg_class__oid, :adnum=>:pg_attribute__attnum).
          left_outer_join(:pg_index, :indrelid=>:pg_class__oid, :indisprimary=>true).
          filter(:pg_attribute__attisdropped=>false).
          filter{|o| o.pg_attribute__attnum > 0}.
          filter(:pg_class__relname=>ds2.send(:input_identifier, table_name)).
          order(:pg_attribute__attnum)
        ds.join!(:pg_namespace, :oid=>:pg_class__relnamespace, :nspname=>(opts[:schema] || default_schema).to_s) if opts[:schema] || default_schema
        ds.identifier_input_method = nil
        ds.identifier_output_method = nil
        ds.map do |row|
          row[:default] = nil if row[:default].blank?
          row[:type] = schema_column_type(row[:db_type])
          [ds2.send(:output_identifier, row.delete(:name)), row]
        end
      end

      # Turns an array of argument specifiers into an SQL fragment used for function arguments.  See create_function_sql.
      def sql_function_args(args)
        "(#{Array(args).map{|a| Array(a).reverse.join(' ')}.join(', ')})"
      end
      
      # Override the standard type conversions with PostgreSQL specific ones
      def type_literal_base(column)
        TYPES[column[:type]]
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
      FOR_UPDATE = ' FOR UPDATE'.freeze
      LOCK = 'LOCK TABLE %s IN %s MODE'.freeze
      NULL = LiteralString.new('NULL').freeze
      PG_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
      ROW_EXCLUSIVE = 'ROW EXCLUSIVE'.freeze
      ROW_SHARE = 'ROW SHARE'.freeze
      SELECT_CLAUSE_ORDER = %w'distinct columns from join where group having compounds order limit lock'.freeze
      SHARE = 'SHARE'.freeze
      SHARE_ROW_EXCLUSIVE = 'SHARE ROW EXCLUSIVE'.freeze
      SHARE_UPDATE_EXCLUSIVE = 'SHARE UPDATE EXCLUSIVE'.freeze
      
      # Shared methods for prepared statements when used with PostgreSQL databases.
      module PreparedStatementMethods
        # Override insert action to use RETURNING if the server supports it.
        def prepared_sql
          return @prepared_sql if @prepared_sql
          super
          if @prepared_type == :insert and server_version >= 80200
            @prepared_sql = insert_returning_pk_sql(@prepared_modify_values)
            meta_def(:insert_returning_pk_sql){|*args| prepared_sql}
          end
          @prepared_sql
        end
      end

      # Return the results of an ANALYZE query as a string
      def analyze(opts = nil)
        analysis = []
        fetch_rows(EXPLAIN_ANALYZE + select_sql(opts)) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
      
      # Return the results of an EXPLAIN query as a string
      def explain(opts = nil)
        analysis = []
        fetch_rows(EXPLAIN + select_sql(opts)) do |r|
          analysis << r[QUERY_PLAN]
        end
        analysis.join("\r\n")
      end
      
      # Return a cloned dataset with a :share lock type.
      def for_share
        clone(:lock => :share)
      end
      
      # Return a cloned dataset with a :update lock type.
      def for_update
        clone(:lock => :update)
      end
      
      # PostgreSQL specific full text search syntax, using tsearch2 (included
      # in 8.3 by default, and available for earlier versions as an add-on).
      def full_text_search(cols, terms, opts = {})
        lang = opts[:language] || 'simple'
        cols =  Array(cols).map{|x| SQL::Function.new(:COALESCE, x, '')}.sql_string_join(' ')
        filter("to_tsvector(#{literal(lang)}, #{literal(cols)}) @@ to_tsquery(#{literal(lang)}, #{literal(Array(terms).join(' | '))})")
      end
      
      # Insert given values into the database.
      def insert(*values)
        if !@opts[:sql] and server_version >= 80200
          single_value(default_server_opts(:sql=>insert_returning_pk_sql(*values)))
        else
          execute_insert(insert_sql(*values), :table=>opts[:from].first,
            :values=>values.size == 1 ? values.first : values)
        end
      end

      # Use the RETURNING clause to return the columns listed in returning.
      def insert_returning_sql(returning, *values)
        "#{insert_sql(*values)} RETURNING #{column_list(Array(returning))}"
      end

      # Insert a record returning the record inserted
      def insert_select(*values)
        single_record(default_server_opts(:naked=>true, :sql=>insert_returning_sql(nil, *values))) if server_version >= 80200
      end

      # Handle microseconds for Time and DateTime values, as well as PostgreSQL
      # specific boolean values and string escaping.
      def literal(v)
        case v
        when LiteralString
          v
        when SQL::Blob
          "'#{v.gsub(/[\000-\037\047\134\177-\377]/){|b| "\\#{("%o" % b[0..1].unpack("C")[0]).rjust(3, '0')}"}}'"
        when String
          "'#{v.gsub("'", "''")}'"
        when Time
          "#{v.strftime(PG_TIMESTAMP_FORMAT)}.#{sprintf("%06d",v.usec)}'"
        when DateTime
          "#{v.strftime(PG_TIMESTAMP_FORMAT)}.#{sprintf("%06d", (v.sec_fraction * 86400000000).to_i)}'"
        when TrueClass
          BOOL_TRUE
        when FalseClass
          BOOL_FALSE
        else
          super
        end
      end
      
      # Locks the table with the specified mode.
      def lock(mode, server=nil)
        sql = LOCK % [source_list(@opts[:from]), mode]
        @db.synchronize(server) do
          if block_given? # perform locking inside a transaction and yield to block
            @db.transaction(server){@db.execute(sql, :server=>server); yield}
          else
            @db.execute(sql, :server=>server) # lock without a transaction
            self
          end
        end
      end
      
      # For PostgreSQL version > 8.2, allow inserting multiple rows at once.
      def multi_insert_sql(columns, values)
        return super if server_version < 80200
        
        # postgresql 8.2 introduces support for multi-row insert
        values = values.map {|r| literal(Array(r))}.join(COMMA_SEPARATOR)
        ["INSERT INTO #{source_list(@opts[:from])} (#{identifier_list(columns)}) VALUES #{values}"]
      end
      
      private
      
      # Use the RETURNING clause to return the primary key of the inserted record, if it exists
      def insert_returning_pk_sql(*values)
        pk = db.primary_key(opts[:from].first)
        insert_returning_sql(pk ? Sequel::SQL::Identifier.new(pk) : NULL, *values)
      end
      
      # PostgreSQL is smart and can use parantheses around all datasets to get
      # the correct answers.
      def select_compounds_sql(sql, opts)
        return unless opts[:compounds]
        opts[:compounds].each do |type, dataset, all|
          sql.replace("(#{sql} #{type.to_s.upcase}#{' ALL' if all} #{subselect_sql(dataset)})")
        end
      end

      # The order of clauses in the SELECT SQL statement
      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # Support lock mode, allowing FOR SHARE and FOR UPDATE queries.
      def select_lock_sql(sql, opts)
        case opts[:lock]
        when :update
          sql << FOR_UPDATE
        when :share
          sql << FOR_SHARE
        end
      end
      
      # The version of the database server
      def server_version
        db.server_version(@opts[:server])
      end
    end
  end
end
