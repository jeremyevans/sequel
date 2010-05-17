module Sequel
  class Database
    # ---------------------
    # :section: Methods that execute queries and/or return results
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
    
    POSTGRES_DEFAULT_RE = /\A(?:B?('.*')::[^']+|\((-?\d+(?:\.\d+)?)\))\z/
    MSSQL_DEFAULT_RE = /\A(?:\(N?('.*')\)|\(\((-?\d+(?:\.\d+)?)\)\))\z/
    MYSQL_TIMESTAMP_RE = /\ACURRENT_(?:DATE|TIMESTAMP)?\z/
    STRING_DEFAULT_RE = /\A'(.*)'\z/

    # The prepared statement objects for this database, keyed by name
    attr_reader :prepared_statements
    
    # Runs the supplied SQL statement string on the database server.
    # Alias for run.
    def <<(sql)
      run(sql)
    end
    
    # Call the prepared statement with the given name with the given hash
    # of arguments.
    def call(ps_name, hash={})
      prepared_statements[ps_name].call(hash)
    end
    
    # Executes the given SQL on the database. This method should be overridden in descendants.
    # This method should not be called directly by user code.
    def execute(sql, opts={})
      raise NotImplementedError, "#execute should be overridden by adapters"
    end
    
    # Method that should be used when submitting any DDL (Data Definition
    # Language) SQL.  By default, calls execute_dui.
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

    # Returns a single value from the database, e.g.:
    #
    #   # SELECT 1
    #   DB.get(1) #=> 1 
    #
    #   # SELECT version()
    #   DB.get(:version.sql_function) #=> ...
    def get(*args, &block)
      dataset.get(*args, &block)
    end
    
    # Runs the supplied SQL statement string on the database server. Returns nil.
    # Options:
    # * :server - The server to run the SQL on.
    def run(sql, opts={})
      execute_ddl(sql, opts)
      nil
    end
    
    # Parse the schema from the database.
    # Returns the schema for the given table as an array with all members being arrays of length 2,
    # the first member being the column name, and the second member being a hash of column information.
    # Available options are:
    #
    # * :reload - Get fresh information from the database, instead of using
    #   cached information.  If table_name is blank, :reload should be used
    #   unless you are sure that schema has not been called before with a
    #   table_name, otherwise you may only getting the schemas for tables
    #   that have been requested explicitly.
    # * :schema - An explicit schema to use.  It may also be implicitly provided
    #   via the table name.
    def schema(table, opts={})
      raise(Error, 'schema parsing is not implemented on this database') unless respond_to?(:schema_parse_table, true)

      sch, table_name = schema_and_table(table)
      quoted_name = quote_schema_table(table)
      opts = opts.merge(:schema=>sch) if sch && !opts.include?(:schema)

      @schemas.delete(quoted_name) if opts[:reload]
      return @schemas[quoted_name] if @schemas[quoted_name]

      cols = schema_parse_table(table_name, opts)
      raise(Error, 'schema parsing returned no columns, table probably doesn\'t exist') if cols.nil? || cols.empty?
      cols.each{|_,c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      @schemas[quoted_name] = cols
    end

    # Returns true if a table with the given name exists.  This requires a query
    # to the database.
    def table_exists?(name)
      begin 
        from(name).first
        true
      rescue
        false
      end
    end
    
    # Starts a database transaction.  When a database transaction is used,
    # either all statements are successful or none of the statements are
    # successful.  Note that MySQL MyISAM tabels do not support transactions.
    #
    # The following options are respected:
    #
    # * :server  - The server to use for the transaction
    # * :savepoint - Whether to create a new savepoint for this transaction,
    #   only respected if the database adapter supports savepoints.  By
    #   default Sequel will reuse an existing transaction, so if you want to
    #   use a savepoint you must use this option.
    def transaction(opts={}, &block)
      synchronize(opts[:server]) do |conn|
        return yield(conn) if already_in_transaction?(conn, opts)
        _transaction(conn, &block)
      end
    end
    
    private
    
    # Internal generic transaction method.  Any exception raised by the given
    # block will cause the transaction to be rolled back.  If the exception is
    # not Sequel::Rollback, the error will be reraised. If no exception occurs
    # inside the block, the transaction is commited.
    def _transaction(conn)
      begin
        add_transaction
        t = begin_transaction(conn)
        yield(conn)
      rescue Exception => e
        rollback_transaction(t) if t
        transaction_error(e)
      ensure
        begin
          commit_transaction(t) unless e
        rescue Exception => e
          raise_error(e, :classes=>database_error_classes)
        ensure
          remove_transaction(t)
        end
      end
    end
    
    # Add the current thread to the list of active transactions
    def add_transaction
      th = Thread.current
      if supports_savepoints?
        unless @transactions.include?(th)
          th[:sequel_transaction_depth] = 0
          @transactions << th
        end
      else
        @transactions << th
      end
    end    

    # Whether the current thread/connection is already inside a transaction
    def already_in_transaction?(conn, opts)
      @transactions.include?(Thread.current) && (!supports_savepoints? || !opts[:savepoint])
    end
    
    # SQL to start a new savepoint
    def begin_savepoint_sql(depth)
      SQL_SAVEPOINT % depth
    end

    # Start a new database transaction on the given connection.
    def begin_transaction(conn)
      if supports_savepoints?
        th = Thread.current
        depth = th[:sequel_transaction_depth]
        conn = transaction_statement_object(conn) if respond_to?(:transaction_statement_object, true)
        log_connection_execute(conn, depth > 0 ? begin_savepoint_sql(depth) : begin_transaction_sql)
        th[:sequel_transaction_depth] += 1
      else
        log_connection_execute(conn, begin_transaction_sql)
      end
      conn
    end
    
    # SQL to BEGIN a transaction.
    def begin_transaction_sql
      SQL_BEGIN
    end

    # Convert the given default, which should be a database specific string, into
    # a ruby object.
    def column_schema_to_ruby_default(default, type)
      return if default.nil?
      orig_default = default
      if database_type == :postgres and m = POSTGRES_DEFAULT_RE.match(default)
        default = m[1] || m[2]
      end
      if database_type == :mssql and m = MSSQL_DEFAULT_RE.match(default)
        default = m[1] || m[2]
      end
      if [:string, :blob, :date, :datetime, :time, :enum].include?(type)
        if database_type == :mysql
          return if [:date, :datetime, :time].include?(type) && MYSQL_TIMESTAMP_RE.match(default)
          orig_default = default = "'#{default.gsub("'", "''").gsub('\\', '\\\\')}'"
        end
        return unless m = STRING_DEFAULT_RE.match(default)
        default = m[1].gsub("''", "'")
      end
      res = begin
        case type
        when :boolean
          case default 
          when /[f0]/i
            false
          when /[t1]/i
            true
          end
        when :string, :enum
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
      rescue
        nil
      end
    end
   
    # SQL to commit a savepoint
    def commit_savepoint_sql(depth)
      SQL_RELEASE_SAVEPOINT % depth
    end

    # Commit the active transaction on the connection
    def commit_transaction(conn)
      if supports_savepoints?
        depth = Thread.current[:sequel_transaction_depth]
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
    def input_identifier_meth
      dataset.method(:input_identifier)
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
    def output_identifier_meth
      dataset.method(:output_identifier)
    end

    # SQL to ROLLBACK a transaction.
    def rollback_transaction_sql
      SQL_ROLLBACK
    end
    
    # Remove the cached schema for the given schema name
    def remove_cached_schema(table)
      @schemas.delete(quote_schema_table(table)) if @schemas
    end
    
    # Remove the current thread from the list of active transactions
    def remove_transaction(conn)
      th = Thread.current
      @transactions.delete(th) if !supports_savepoints? || ((th[:sequel_transaction_depth] -= 1) <= 0)
    end

    # SQL to rollback to a savepoint
    def rollback_savepoint_sql(depth)
      SQL_ROLLBACK_TO_SAVEPOINT % depth
    end

    # Rollback the active transaction on the connection
    def rollback_transaction(conn)
      if supports_savepoints?
        depth = Thread.current[:sequel_transaction_depth]
        log_connection_execute(conn, depth > 1 ? rollback_savepoint_sql(depth-1) : rollback_transaction_sql)
      else
        log_connection_execute(conn, rollback_transaction_sql)
      end
    end

    # Match the database's column type to a ruby type via a
    # regular expression.  The following ruby types are supported:
    # integer, string, date, datetime, boolean, and float.
    def schema_column_type(db_type)
      case db_type
      when /\Ainterval\z/io
        :interval
      when /\A(character( varying)?|n?(var)?char|n?text)/io
        :string
      when /\A(int(eger)?|(big|small|tiny)int)/io
        :integer
      when /\Adate\z/io
        :date
      when /\A((small)?datetime|timestamp( with(out)? time zone)?)\z/io
        :datetime
      when /\Atime( with(out)? time zone)?\z/io
        :time
      when /\A(boolean|bit)\z/io
        :boolean
      when /\A(real|float|double( precision)?)\z/io
        :float
      when /\A(?:(?:(?:num(?:ber|eric)?|decimal)(?:\(\d+,\s*(\d+)\))?)|(?:small)?money)\z/io
        $1 && $1 == '0' ? :integer : :decimal
      when /bytea|blob|image|(var)?binary/io
        :blob
      when /\Aenum/io
        :enum
      end
    end

    # Raise a database error unless the exception is an Rollback.
    def transaction_error(e)
      raise_error(e, :classes=>database_error_classes) unless e.is_a?(Rollback)
    end

  end
end
