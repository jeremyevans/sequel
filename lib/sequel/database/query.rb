module Sequel
  class Database
    # ---------------------
    # :section: 1 - Methods that execute queries and/or return results
    # This methods generally execute SQL code on the database server.
    # ---------------------

    SQL_BEGIN = 'BEGIN'.freeze
    SQL_COMMIT = 'COMMIT'.freeze
    SQL_RELEASE_SAVEPOINT = 'RELEASE SAVEPOINT autopoint_%d'.freeze
    SQL_ROLLBACK = 'ROLLBACK'.freeze
    SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze
    SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
    
    TRANSACTION_BEGIN = 'Transaction.begin'.freeze
    TRANSACTION_COMMIT = 'Transaction.commit'.freeze
    TRANSACTION_ROLLBACK = 'Transaction.rollback'.freeze

    TRANSACTION_ISOLATION_LEVELS = {:uncommitted=>'READ UNCOMMITTED'.freeze,
      :committed=>'READ COMMITTED'.freeze,
      :repeatable=>'REPEATABLE READ'.freeze,
      :serializable=>'SERIALIZABLE'.freeze}
    
    STRING_DEFAULT_RE = /\A'(.*)'\z/
    CURRENT_TIMESTAMP_RE = /now|CURRENT|getdate|\ADate\(\)\z/io
    COLUMN_SCHEMA_DATETIME_TYPES = [:date, :datetime]
    COLUMN_SCHEMA_STRING_TYPES = [:string, :blob, :date, :datetime, :time, :enum, :set, :interval]

    # The prepared statement object hash for this database, keyed by name symbol
    attr_reader :prepared_statements
    
    # The default transaction isolation level for this database,
    # used for all future transactions.  For MSSQL, this should be set
    # to something if you ever plan to use the :isolation option to
    # Database#transaction, as on MSSQL if affects all future transactions
    # on the same connection.
    attr_accessor :transaction_isolation_level

    # Whether the schema should be cached for this database.  True by default
    # for performance, can be set to false to always issue a database query to
    # get the schema.
    attr_accessor :cache_schema
    
    # Runs the supplied SQL statement string on the database server.
    # Returns self so it can be safely chained:
    #
    #   DB << "UPDATE albums SET artist_id = NULL" << "DROP TABLE artists"
    def <<(sql)
      run(sql)
      self
    end
    
    # Call the prepared statement with the given name with the given hash
    # of arguments.
    #
    #   DB[:items].filter(:id=>1).prepare(:first, :sa)
    #   DB.call(:sa) # SELECT * FROM items WHERE id = 1
    def call(ps_name, hash={}, &block)
      prepared_statement(ps_name).call(hash, &block)
    end
    
    # Executes the given SQL on the database. This method should be overridden in descendants.
    # This method should not be called directly by user code.
    def execute(sql, opts={})
      raise NotImplemented, "#execute should be overridden by adapters"
    end
    
    # Method that should be used when submitting any DDL (Data Definition
    # Language) SQL, such as +create_table+.  By default, calls +execute_dui+.
    # This method should not be called directly by user code.
    def execute_ddl(sql, opts={}, &block)
      execute_dui(sql, opts, &block)
    end

    # Method that should be used when issuing a DELETE, UPDATE, or INSERT
    # statement.  By default, calls execute.
    # This method should not be called directly by user code.
    def execute_dui(sql, opts={}, &block)
      execute(sql, opts, &block)
    end

    # Method that should be used when issuing a INSERT
    # statement.  By default, calls execute_dui.
    # This method should not be called directly by user code.
    def execute_insert(sql, opts={}, &block)
      execute_dui(sql, opts, &block)
    end

    # Returns an array of hashes containing foreign key information from the
    # table.  Each hash will contain at least the following fields:
    #
    # :columns :: An array of columns in the given table
    # :table :: The table referenced by the columns
    # :key :: An array of columns referenced (in the table specified by :table),
    #         but can be nil on certain adapters if the primary key is referenced.
    #
    # The hash may also contain entries for:
    #
    # :deferrable :: Whether the constraint is deferrable
    # :name :: The name of the constraint
    # :on_delete :: The action to take ON DELETE
    # :on_update :: The action to take ON UPDATE
    def foreign_key_list(table, opts={})
      raise NotImplemented, "#foreign_key_list should be overridden by adapters"
    end
    
    # Returns a single value from the database, e.g.:
    #
    #   DB.get(1) # SELECT 1
    #   # => 1
    #   DB.get{server_version{}} # SELECT server_version()
    def get(*args, &block)
      dataset.get(*args, &block)
    end
    
    # Return a hash containing index information for the table. Hash keys are index name symbols.
    # Values are subhashes with two keys, :columns and :unique.  The value of :columns
    # is an array of symbols of column names.  The value of :unique is true or false
    # depending on if the index is unique.
    #
    # Should not include the primary key index, functional indexes, or partial indexes.
    #
    #   DB.indexes(:artists)
    #   # => {:artists_name_ukey=>{:columns=>[:name], :unique=>true}}
    def indexes(table, opts={})
      raise NotImplemented, "#indexes should be overridden by adapters"
    end
    
    # Runs the supplied SQL statement string on the database server. Returns nil.
    # Options:
    # :server :: The server to run the SQL on.
    #
    #   DB.run("SET some_server_variable = 42")
    def run(sql, opts={})
      execute_ddl(sql, opts)
      nil
    end
    
    # Returns the schema for the given table as an array with all members being arrays of length 2,
    # the first member being the column name, and the second member being a hash of column information.
    # The table argument can also be a dataset, as long as it only has one table.
    # Available options are:
    #
    # :reload :: Ignore any cached results, and get fresh information from the database.
    # :schema :: An explicit schema to use.  It may also be implicitly provided
    #            via the table name.
    #
    # If schema parsing is supported by the database, the column information should hash at least contain the
    # following entries:
    #
    # :allow_null :: Whether NULL is an allowed value for the column.
    # :db_type :: The database type for the column, as a database specific string.
    # :default :: The database default for the column, as a database specific string.
    # :primary_key :: Whether the columns is a primary key column.  If this column is not present,
    #                 it means that primary key information is unavailable, not that the column
    #                 is not a primary key.
    # :ruby_default :: The database default for the column, as a ruby object.  In many cases, complex
    #                  database defaults cannot be parsed into ruby objects, in which case nil will be
    #                  used as the value.
    # :type :: A symbol specifying the type, such as :integer or :string.
    #
    # Example:
    #
    #   DB.schema(:artists)
    #   # [[:id,
    #   #   {:type=>:integer,
    #   #    :primary_key=>true,
    #   #    :default=>"nextval('artist_id_seq'::regclass)",
    #   #    :ruby_default=>nil,
    #   #    :db_type=>"integer",
    #   #    :allow_null=>false}],
    #   #  [:name,
    #   #   {:type=>:string,
    #   #    :primary_key=>false,
    #   #    :default=>nil,
    #   #    :ruby_default=>nil,
    #   #    :db_type=>"text",
    #   #    :allow_null=>false}]]
    def schema(table, opts={})
      raise(Error, 'schema parsing is not implemented on this database') unless respond_to?(:schema_parse_table, true)

      opts = opts.dup
      tab = if table.is_a?(Dataset)
        o = table.opts
        from = o[:from]
        raise(Error, "can only parse the schema for a dataset with a single from table") unless from && from.length == 1 && !o.include?(:join) && !o.include?(:sql)
        table.first_source_table
      else
        table
      end

      qualifiers = split_qualifiers(tab)
      table_name = qualifiers.pop
      sch = qualifiers.pop
      information_schema_schema = case qualifiers.length
      when 1
        Sequel.identifier(*qualifiers)
      when 2
        Sequel.qualify(*qualifiers)
      end

      if table.is_a?(Dataset)
        quoted_name = table.literal(tab)
        opts[:dataset] = table
      else
        quoted_name = schema_utility_dataset.literal(table)
      end

      opts[:schema] = sch if sch && !opts.include?(:schema)
      opts[:information_schema_schema] = information_schema_schema if information_schema_schema && !opts.include?(:information_schema_schema)

      Sequel.synchronize{@schemas.delete(quoted_name)} if opts[:reload]
      if v = Sequel.synchronize{@schemas[quoted_name]}
        return v
      end

      cols = schema_parse_table(table_name, opts)
      raise(Error, 'schema parsing returned no columns, table probably doesn\'t exist') if cols.nil? || cols.empty?
      cols.each{|_,c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      Sequel.synchronize{@schemas[quoted_name] = cols} if cache_schema
      cols
    end

    # Returns true if a table with the given name exists.  This requires a query
    # to the database.
    #
    #   DB.table_exists?(:foo) # => false
    #   # SELECT NULL FROM foo LIMIT 1
    #
    # Note that since this does a SELECT from the table, it can give false negatives
    # if you don't have permission to SELECT from the table.
    def table_exists?(name)
      sch, table_name = schema_and_table(name)
      name = SQL::QualifiedIdentifier.new(sch, table_name) if sch
      _table_exists?(from(name))
      true
    rescue DatabaseError
      false
    end

    # Return all tables in the database as an array of symbols.
    #
    #   DB.tables # => [:albums, :artists]
    def tables(opts={})
      raise NotImplemented, "#tables should be overridden by adapters"
    end
    
    # Starts a database transaction.  When a database transaction is used,
    # either all statements are successful or none of the statements are
    # successful.  Note that MySQL MyISAM tables do not support transactions.
    #
    # The following general options are respected:
    #
    # :disconnect :: If set to :retry, automatically sets the :retry_on option
    #                with a Sequel::DatabaseDisconnectError.  This option is only
    #                present for backwards compatibility, please use the :retry_on
    #                option instead.
    # :isolation :: The transaction isolation level to use for this transaction,
    #               should be :uncommitted, :committed, :repeatable, or :serializable,
    #               used if given and the database/adapter supports customizable
    #               transaction isolation levels.
    # :num_retries :: The number of times to retry if the :retry_on option is used.
    #                 The default is 5 times.  Can be set to nil to retry indefinitely,
    #                 but that is not recommended.
    # :prepare :: A string to use as the transaction identifier for a
    #             prepared transaction (two-phase commit), if the database/adapter
    #             supports prepared transactions.
    # :retry_on :: An exception class or array of exception classes for which to
    #              automatically retry the transaction.  Can only be set if not inside
    #              an existing transaction.
    #              Note that this should not be used unless the entire transaction
    #              block is idempotent, as otherwise it can cause non-idempotent
    #              behavior to execute multiple times.
    # :rollback :: Can the set to :reraise to reraise any Sequel::Rollback exceptions
    #              raised, or :always to always rollback even if no exceptions occur
    #              (useful for testing).
    # :server :: The server to use for the transaction.
    # :savepoint :: Whether to create a new savepoint for this transaction,
    #               only respected if the database/adapter supports savepoints.  By
    #               default Sequel will reuse an existing transaction, so if you want to
    #               use a savepoint you must use this option.
    #
    # PostgreSQL specific options:
    #
    # :deferrable :: (9.1+) If present, set to DEFERRABLE if true or NOT DEFERRABLE if false.
    # :read_only :: If present, set to READ ONLY if true or READ WRITE if false.
    # :synchronous :: if non-nil, set synchronous_commit
    #                 appropriately.  Valid values true, :on, false, :off, :local (9.1+),
    #                 and :remote_write (9.2+).
    def transaction(opts={}, &block)
      if opts[:disconnect] == :retry
        raise(Error, 'cannot specify both :disconnect=>:retry and :retry_on') if opts[:retry_on]
        return transaction(opts.merge(:retry_on=>Sequel::DatabaseDisconnectError, :disconnect=>nil), &block)
      end

      if retry_on = opts[:retry_on]
        num_retries = opts.fetch(:num_retries, 5)
        begin
          transaction(opts.merge(:retry_on=>nil, :retrying=>true), &block)
        rescue *retry_on
          if num_retries
            num_retries -= 1
            retry if num_retries >= 0
          else
            retry
          end
          raise
        end
      else
        synchronize(opts[:server]) do |conn|
          if already_in_transaction?(conn, opts)
            if opts[:retrying]
              raise Sequel::Error, "cannot set :disconnect=>:retry or :retry_on options if you are already inside a transaction"
            end
            return yield(conn)
          end
          _transaction(conn, opts, &block)
        end
      end
    end
    
    # Return all views in the database as an array of symbols.
    #
    #   DB.views # => [:gold_albums, :artists_with_many_albums]
    def views(opts={})
      raise NotImplemented, "#views should be overridden by adapters"
    end
    
    private
    
    # Should raise an error if the table doesn't not exist,
    # and not raise an error if the table does exist.
    def _table_exists?(ds)
      ds.get(Sequel::NULL)
    end
    
    # Internal generic transaction method.  Any exception raised by the given
    # block will cause the transaction to be rolled back.  If the exception is
    # not a Sequel::Rollback, the error will be reraised. If no exception occurs
    # inside the block, the transaction is commited.
    def _transaction(conn, opts={})
      rollback = opts[:rollback]
      begin
        add_transaction(conn, opts)
        begin_transaction(conn, opts)
        if rollback == :always
          begin
            yield(conn)
          rescue Exception => e1
            raise e1
          ensure
            raise ::Sequel::Rollback unless e1
          end
        else
          yield(conn)
        end
      rescue Exception => e
        begin
          rollback_transaction(conn, opts)
        rescue Exception => e3
          raise_error(e3, :classes=>database_error_classes, :conn=>conn)
        end
        transaction_error(e, :conn=>conn, :rollback=>rollback)
      ensure
        begin
          committed = commit_or_rollback_transaction(e, conn, opts)
        rescue Exception => e2
          begin
            raise_error(e2, :classes=>database_error_classes, :conn=>conn)
          rescue Sequel::DatabaseError => e4
            begin
              rollback_transaction(conn, opts)
            ensure
              raise e4
            end
          end
        ensure
          remove_transaction(conn, committed)
        end
      end
    end

    # Synchronize access to the current transactions, returning the hash
    # of options for the current transaction (if any)
    def _trans(conn)
      Sequel.synchronize{@transactions[conn]}
    end

    # Add the current thread to the list of active transactions
    def add_transaction(conn, opts)
      if supports_savepoints?
        unless _trans(conn)
          if (prep = opts[:prepare]) && supports_prepared_transactions?
            Sequel.synchronize{@transactions[conn] = {:savepoint_level=>0, :prepare=>prep}}
          else
            Sequel.synchronize{@transactions[conn] = {:savepoint_level=>0}}
          end
        end
      elsif (prep = opts[:prepare]) && supports_prepared_transactions?
        Sequel.synchronize{@transactions[conn] = {:prepare => prep}}
      else
        Sequel.synchronize{@transactions[conn] = {}}
      end
    end    

    # Call all stored after_commit blocks for the given transaction
    def after_transaction_commit(conn)
      if ary = _trans(conn)[:after_commit]
        ary.each{|b| b.call}
      end
    end

    # Call all stored after_rollback blocks for the given transaction
    def after_transaction_rollback(conn)
      if ary = _trans(conn)[:after_rollback]
        ary.each{|b| b.call}
      end
    end

    # Whether the current thread/connection is already inside a transaction
    def already_in_transaction?(conn, opts)
      _trans(conn) && (!supports_savepoints? || !opts[:savepoint])
    end
    
    # SQL to start a new savepoint
    def begin_savepoint_sql(depth)
      SQL_SAVEPOINT % depth
    end

    # Start a new database connection on the given connection
    def begin_new_transaction(conn, opts)
      log_connection_execute(conn, begin_transaction_sql)
      set_transaction_isolation(conn, opts)
    end

    # Start a new database transaction or a new savepoint on the given connection.
    def begin_transaction(conn, opts={})
      if supports_savepoints?
        th = _trans(conn)
        if (depth = th[:savepoint_level]) > 0
          log_connection_execute(conn, begin_savepoint_sql(depth))
        else
          begin_new_transaction(conn, opts)
        end
        th[:savepoint_level] += 1
      else
        begin_new_transaction(conn, opts)
      end
    end
    
    # SQL to BEGIN a transaction.
    def begin_transaction_sql
      SQL_BEGIN
    end

    # Whether the type should be treated as a string type when parsing the
    # column schema default value.
    def column_schema_default_string_type?(type)
      COLUMN_SCHEMA_STRING_TYPES.include?(type)
    end

    # Transform the given normalized default string into a ruby object for the
    # given type.
    def column_schema_default_to_ruby_value(default, type)
      case type
      when :boolean
        case default 
        when /[f0]/i
          false
        when /[t1]/i
          true
        end
      when :string, :enum, :set, :interval
        default
      when :blob
        Sequel::SQL::Blob.new(default)
      when :integer
        Integer(default)
      when :float
        Float(default)
      when :date
        Sequel.string_to_date(default)
      when :datetime
        DateTime.parse(default)
      when :time
        Sequel.string_to_time(default)
      when :decimal
        BigDecimal.new(default)
      end
    end
   
    # Normalize the default value string for the given type
    # and return the normalized value.
    def column_schema_normalize_default(default, type)
      if column_schema_default_string_type?(type)
        return unless m = STRING_DEFAULT_RE.match(default)
        m[1].gsub("''", "'")
      else
        default
      end
    end

    # Convert the given default, which should be a database specific string, into
    # a ruby object.
    def column_schema_to_ruby_default(default, type)
      return default unless default.is_a?(String)
      if COLUMN_SCHEMA_DATETIME_TYPES.include?(type)
        if CURRENT_TIMESTAMP_RE.match(default)
          if type == :date
            return Sequel::CURRENT_DATE
          else
            return Sequel::CURRENT_TIMESTAMP
          end
        end
      end
      default = column_schema_normalize_default(default, type)
      column_schema_default_to_ruby_value(default, type) rescue nil
    end

    if (! defined?(RUBY_ENGINE) or RUBY_ENGINE == 'ruby' or RUBY_ENGINE == 'rbx') and RUBY_VERSION < '1.9'
    # :nocov:
      # Whether to commit the current transaction. On ruby 1.8 and rubinius,
      # Thread.current.status is checked because Thread#kill skips rescue
      # blocks (so exception would be nil), but the transaction should
      # still be rolled back.
      def commit_or_rollback_transaction(exception, conn, opts)
        if exception
          false
        else
          if Thread.current.status == 'aborting'
            rollback_transaction(conn, opts)
            false
          else
            commit_transaction(conn, opts)
            true
          end
        end
      end
    # :nocov:
    else
      # Whether to commit the current transaction.  On ruby 1.9 and JRuby,
      # transactions will be committed if Thread#kill is used on an thread
      # that has a transaction open, and there isn't a work around.
      def commit_or_rollback_transaction(exception, conn, opts)
        if exception
          false
        else
          commit_transaction(conn, opts)
          true
        end
      end
    end
    
    # SQL to commit a savepoint
    def commit_savepoint_sql(depth)
      SQL_RELEASE_SAVEPOINT % depth
    end

    # Commit the active transaction on the connection
    def commit_transaction(conn, opts={})
      if supports_savepoints?
        depth = _trans(conn)[:savepoint_level]
        log_connection_execute(conn, depth > 1 ? commit_savepoint_sql(depth-1) : commit_transaction_sql)
      else
        log_connection_execute(conn, commit_transaction_sql)
      end
    end

    # SQL to COMMIT a transaction.
    def commit_transaction_sql
      SQL_COMMIT
    end
    
    # Method called on the connection object to execute SQL on the database,
    # used by the transaction code.
    def connection_execute_method
      :execute
    end

    # Return a Method object for the dataset's output_identifier_method.
    # Used in metadata parsing to make sure the returned information is in the
    # correct format.
    def input_identifier_meth(ds=nil)
      (ds || dataset).method(:input_identifier)
    end
    
    # Return a dataset that uses the default identifier input and output methods
    # for this database.  Used when parsing metadata so that column symbols are
    # returned as expected.
    def metadata_dataset
      return @metadata_dataset if @metadata_dataset
      ds = dataset
      ds.identifier_input_method = identifier_input_method_default
      ds.identifier_output_method = identifier_output_method_default
      @metadata_dataset = ds
    end

    # Return a Method object for the dataset's output_identifier_method.
    # Used in metadata parsing to make sure the returned information is in the
    # correct format.
    def output_identifier_meth(ds=nil)
      (ds || dataset).method(:output_identifier)
    end

    # Remove the cached schema for the given schema name
    def remove_cached_schema(table)
      Sequel.synchronize{@schemas.delete(quote_schema_table(table))} if @schemas
    end
    
    # Remove the current thread from the list of active transactions
    def remove_transaction(conn, committed)
      if !supports_savepoints? || ((_trans(conn)[:savepoint_level] -= 1) <= 0)
        begin
          if committed
            after_transaction_commit(conn)
          else
            after_transaction_rollback(conn)
          end
        ensure
          Sequel.synchronize{@transactions.delete(conn)}
        end
      end
    end

    # SQL to rollback to a savepoint
    def rollback_savepoint_sql(depth)
      SQL_ROLLBACK_TO_SAVEPOINT % depth
    end

    # Rollback the active transaction on the connection
    def rollback_transaction(conn, opts={})
      if supports_savepoints?
        depth = _trans(conn)[:savepoint_level]
        log_connection_execute(conn, depth > 1 ? rollback_savepoint_sql(depth-1) : rollback_transaction_sql)
      else
        log_connection_execute(conn, rollback_transaction_sql)
      end
    end

    # SQL to ROLLBACK a transaction.
    def rollback_transaction_sql
      SQL_ROLLBACK
    end
    
    # Match the database's column type to a ruby type via a
    # regular expression, and return the ruby type as a symbol
    # such as :integer or :string.
    def schema_column_type(db_type)
      case db_type
      when /\A(character( varying)?|n?(var)?char|n?text|string|clob)/io
        :string
      when /\A(int(eger)?|(big|small|tiny)int)/io
        :integer
      when /\Adate\z/io
        :date
      when /\A((small)?datetime|timestamp( with(out)? time zone)?)(\(\d+\))?\z/io
        :datetime
      when /\Atime( with(out)? time zone)?\z/io
        :time
      when /\A(bool(ean)?)\z/io
        :boolean
      when /\A(real|float|double( precision)?|double\(\d+,\d+\)( unsigned)?)\z/io
        :float
      when /\A(?:(?:(?:num(?:ber|eric)?|decimal)(?:\(\d+,\s*(\d+|false|true)\))?)|(?:small)?money)\z/io
        $1 && ['0', 'false'].include?($1) ? :integer : :decimal
      when /bytea|blob|image|(var)?binary/io
        :blob
      when /\Aenum/io
        :enum
      end
    end

    # Set the transaction isolation level on the given connection
    def set_transaction_isolation(conn, opts)
      if supports_transaction_isolation_levels? and level = opts.fetch(:isolation, transaction_isolation_level)
        log_connection_execute(conn, set_transaction_isolation_sql(level))
      end
    end

    # SQL to set the transaction isolation level
    def set_transaction_isolation_sql(level)
      "SET TRANSACTION ISOLATION LEVEL #{TRANSACTION_ISOLATION_LEVELS[level]}"
    end

    # Raise a database error unless the exception is an Rollback.
    def transaction_error(e, opts={})
      if e.is_a?(Rollback)
        raise e if opts[:rollback] == :reraise
      else
        raise_error(e, opts.merge(:classes=>database_error_classes))
      end
    end
  end
end
