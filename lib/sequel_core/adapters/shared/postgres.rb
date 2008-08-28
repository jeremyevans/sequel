module Sequel
  module Postgres
    # Array of exceptions that need to be converted.  JDBC
    # uses NativeExceptions, the native adapter uses PGError.
    CONVERTED_EXCEPTIONS = []
    
    # Methods shared by adapter/connection instances.
    module AdapterMethods
      attr_writer :db
      
      SELECT_CURRVAL = "SELECT currval('%s')".freeze
      SELECT_CUSTOM_SEQUENCE = <<-end_sql
        SELECT CASE  
            WHEN split_part(def.adsrc, '''', 2) ~ '.' THEN  
              substr(split_part(def.adsrc, '''', 2),  
                     strpos(split_part(def.adsrc, '''', 2), '.')+1) 
            ELSE split_part(def.adsrc, '''', 2)  
          END
        FROM pg_class t
        JOIN pg_namespace  name ON (t.relnamespace = name.oid)
        JOIN pg_attribute  attr ON (t.oid = attrelid)
        JOIN pg_attrdef    def  ON (adrelid = attrelid AND adnum = attnum)
        JOIN pg_constraint cons ON (conrelid = adrelid AND adnum = conkey[1])
        WHERE t.oid = '%s'::regclass
          AND cons.contype = 'p'
          AND def.adsrc ~* 'nextval'
      end_sql
      SELECT_PK = <<-end_sql
        SELECT pg_attribute.attname
        FROM pg_class, pg_attribute, pg_index
        WHERE pg_class.oid = pg_attribute.attrelid AND
          pg_class.oid = pg_index.indrelid AND
          pg_index.indkey[0] = pg_attribute.attnum AND
          pg_index.indisprimary = 't' AND
          pg_class.relname = '%s'
      end_sql
      SELECT_SERIAL_SEQUENCE = <<-end_sql
        SELECT seq.relname
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
          AND dep.refobjid = '%s'::regclass
      end_sql
      
      # Depth of the current transaction on this connection, used
      # to implement multi-level transactions with savepoints.
      attr_accessor :transaction_depth
      
      # Get the last inserted value for the given sequence.
      def last_insert_id(sequence)
        sql = SELECT_CURRVAL % sequence
        @db.log_info(sql)
        execute(sql) do |r|
          val = single_value(r)
          return val.to_i if val
        end
      end
      
      # Get the primary key and sequence for the given table.
      def sequence(table)
        sql = SELECT_SERIAL_SEQUENCE % table
        @db.log_info(sql)
        execute(sql) do |r|
          seq = single_value(r)
          return seq if seq
        end
        
        sql = SELECT_CUSTOM_SEQUENCE % table
        @db.log_info(sql)
        execute(sql) do |r|
          return single_value(r)
        end
      end
      
      # Get the primary key for the given table.
      def primary_key(table)
        sql = SELECT_PK % table
        @db.log_info(sql)
        execute(sql) do |r|
          return single_value(r)
        end
      end
    end
    
    # Methods shared by Database instances that connect to PostgreSQL.
    module DatabaseMethods
      PREPARED_ARG_PLACEHOLDER = '$'.lit.freeze
      RE_CURRVAL_ERROR = /currval of sequence "(.*)" is not yet defined in this session|relation "(.*)" does not exist/.freeze
      RELATION_QUERY = {:from => [:pg_class], :select => [:relname]}.freeze
      RELATION_FILTER = "(relkind = 'r') AND (relname !~ '^pg|sql')".freeze
      SQL_BEGIN = 'BEGIN'.freeze
      SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
      SQL_COMMIT = 'COMMIT'.freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze
      SQL_ROLLBACK = 'ROLLBACK'.freeze
      SQL_RELEASE_SAVEPOINT = 'RELEASE SAVEPOINT autopoint_%d'.freeze
      SYSTEM_TABLE_REGEXP = /^pg|sql/.freeze
      
      # Remove the cached entries for primary keys and sequences when dropping a table.
      def drop_table(*names)
        names.each do |name|
          s = name.to_sym
          @primary_keys.delete(s)
          @primary_key_sequences.delete(s)
        end
        super
      end

      # Always CASCADE the table drop
      def drop_table_sql(name)
        "DROP TABLE #{name} CASCADE"
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
          lang = index[:language] ? "#{literal(index[:language])}, " : ""
          cols = index[:columns].map {|c| literal(c)}.join(" || ")
          expr = "(to_tsvector(#{lang}#{cols}))"
          index_type = :gin
        when :spatial
          index_type = :gist
        end
        "CREATE #{unique}INDEX #{index_name} ON #{table_name} #{"USING #{index_type} " if index_type}#{expr}#{filter}"
      end
      
      # Dataset containing all current database locks 
      def locks
        dataset.from(:pg_class, :pg_locks).
          select(:pg_class__relname, :pg_locks.*).
          filter(:pg_class__relfilenode=>:pg_locks__relation)
      end
      
      # Return primary key for the given table.
      def primary_key(table, server=nil)
        synchronize(server){|conn| primary_key_for_table(conn, table)}
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
          m = /PostgreSQL (\d+)\.(\d+)\.(\d+)/.match(get(:version[]))
          @server_version = (m[1].to_i * 10000) + (m[2].to_i * 100) + m[3].to_i
        end
        @server_version
      end
      
      # Array of symbols specifying table names in the current database.
      def tables
        dataset(RELATION_QUERY).filter(RELATION_FILTER).map{|r| r[:relname].to_sym}
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
      
      # The result of the insert for the given table and values.  If values
      # is an array, assume the first column is the primary key and return
      # that.  If values is a hash, lookup the primary key for the table.  If
      # the primary key is present in the hash, return its value.  Otherwise,
      # look up the sequence for the table's primary key.  If one exists,
      # return the last value the of the sequence for the connection.
      def insert_result(conn, table, values)
        case values
        when Hash
          return nil unless pk = primary_key_for_table(conn, table)
          if pk and pkv = values[pk.to_sym]
            pkv
          else
            begin
              if seq = primary_key_sequence_for_table(conn, table)
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
      
      # Returns primary key for the given table.  This information is
      # cached, and if the primary key for a table is changed, the
      # @primary_keys instance variable should be reset manually.
      def primary_key_for_table(conn, table)
        @primary_keys.include?(table) ? @primary_keys[table] : (@primary_keys[table] = conn.primary_key(table))
      end
      
      # Returns primary key for the given table.  This information is
      # cached, and if the primary key for a table is changed, the
      # @primary_keys instance variable should be reset manually.
      def primary_key_sequence_for_table(conn, table)
        @primary_key_sequences.include?(table) ? @primary_key_sequences[table] : (@primary_key_sequences[table] = conn.sequence(table))
      end
      
      # When the :schema option is used, use the the given schema.
      # When the :schema option is nil, return results for all schemas.
      # If the :schema option is not used, use the public schema.
      def schema_ds_filter(table_name, opts)
        filt = super
        # Restrict it to the given or public schema, unless specifically requesting :schema = nil
        filt = SQL::BooleanExpression.new(:AND, filt, {:c__table_schema=>opts[:schema] || 'public'}) if opts[:schema] || !opts.include?(:schema)
        filt
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
      PG_TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S".freeze
      QUERY_PLAN = 'QUERY PLAN'.to_sym
      ROW_EXCLUSIVE = 'ROW EXCLUSIVE'.freeze
      ROW_SHARE = 'ROW SHARE'.freeze
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
        lang = opts[:language] ? "#{literal(opts[:language])}, " : ""
        cols = cols.is_a?(Array) ? cols.map {|c| literal(c)}.join(" || ") : literal(cols)
        terms = terms.is_a?(Array) ? literal(terms.join(" | ")) : literal(terms)
        filter("to_tsvector(#{lang}#{cols}) @@ to_tsquery(#{lang}#{terms})")
      end
      
      # Insert given values into the database.
      def insert(*values)
        if !@opts[:sql] and server_version >= 80200
          single_value(:sql=>insert_returning_pk_sql(*values))
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
        single_record(:naked=>true, :sql=>insert_returning_sql(nil, *values)) if server_version >= 80200
      end

      # Handle microseconds for Time and DateTime values, as well as PostgreSQL
      # specific boolean values and string escaping.
      def literal(v)
        case v
        when LiteralString
          v
        when String
          db.synchronize{|c| "'#{SQL::Blob === v ? c.escape_bytea(v) : c.escape_string(v)}'"}
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
        columns = column_list(columns)
        values = values.map {|r| literal(Array(r))}.join(COMMA_SEPARATOR)
        ["INSERT INTO #{source_list(@opts[:from])} (#{columns}) VALUES #{values}"]
      end
      
      # PostgreSQL assumes unquoted identifiers are lower case by default,
      # so do not upcase the identifier when quoting it.
      def quoted_identifier(c)
        "\"#{c}\""
      end
      
      # Support lock mode, allowing FOR SHARE and FOR UPDATE queries.
      def select_sql(opts = nil)
        row_lock_mode = opts ? opts[:lock] : @opts[:lock]
        sql = super
        case row_lock_mode
        when :update
          sql << FOR_UPDATE
        when :share
          sql << FOR_SHARE
        end
        sql
      end
      
      private
      
      # Call execute_insert on the database object with the given values.
      def execute_insert(sql, opts={})
        @db.execute_insert(sql, {:server=>@opts[:server] || :default}.merge(opts))
      end

      # Use the RETURNING clause to return the primary key of the inserted record, if it exists
      def insert_returning_pk_sql(*values)
        pk = db.primary_key(opts[:from].first)
        insert_returning_sql(pk ? Sequel::SQL::Identifier.new(pk) : 'NULL'.lit, *values)
      end

      # The version of the database server
      def server_version
        db.server_version(@opts[:server])
      end
    end
  end
end
