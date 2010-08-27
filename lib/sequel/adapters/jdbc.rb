require 'java'
Sequel.require 'adapters/utils/stored_procedures'

module Sequel
  # Houses Sequel's JDBC support when running on JRuby.
  module JDBC
    # Make it accesing the java.lang hierarchy more ruby friendly.
    module JavaLang
      include_package 'java.lang'
    end
    
    # Make it accesing the java.sql hierarchy more ruby friendly.
    module JavaSQL
      include_package 'java.sql'
    end

    # Make it accesing the javax.naming hierarchy more ruby friendly.
    module JavaxNaming
      include_package 'javax.naming'
    end

    # Used to identify a jndi connection and to extract the jndi
    # resource name.
    JNDI_URI_REGEXP = /\Ajdbc:jndi:(.+)/
    
    # The types to check for 0 scale to transform :decimal types
    # to :integer.
    DECIMAL_TYPE_RE = /number|numeric|decimal/io
    
    # Contains procs keyed on sub adapter type that extend the
    # given database object so it supports the correct database type.
    DATABASE_SETUP = {:postgresql=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/postgresql'
        db.extend(Sequel::JDBC::Postgres::DatabaseMethods)
        JDBC.load_gem('postgres')
        org.postgresql.Driver
      end,
      :mysql=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/mysql'
        db.extend(Sequel::JDBC::MySQL::DatabaseMethods)
        JDBC.load_gem('mysql')
        com.mysql.jdbc.Driver
      end,
      :sqlite=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/sqlite'
        db.extend(Sequel::JDBC::SQLite::DatabaseMethods)
        JDBC.load_gem('sqlite3')
        org.sqlite.JDBC
      end,
      :oracle=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/oracle'
        db.extend(Sequel::JDBC::Oracle::DatabaseMethods)
        Java::oracle.jdbc.driver.OracleDriver
      end,
      :sqlserver=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/mssql'
        db.extend(Sequel::JDBC::MSSQL::DatabaseMethods)
        com.microsoft.sqlserver.jdbc.SQLServerDriver
      end,
      :jtds=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/mssql'
        db.extend(Sequel::JDBC::MSSQL::DatabaseMethods)
        JDBC.load_gem('jtds')
        Java::net.sourceforge.jtds.jdbc.Driver
      end,
      :h2=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/h2'
        db.extend(Sequel::JDBC::H2::DatabaseMethods)
        JDBC.load_gem('h2')
        org.h2.Driver
      end,
      :as400=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/as400'
        db.extend(Sequel::JDBC::AS400::DatabaseMethods)
        com.ibm.as400.access.AS400JDBCDriver
      end
    }
    
    # Allowing loading the necessary JDBC support via a gem, which
    # works for PostgreSQL, MySQL, and SQLite.
    def self.load_gem(name)
      begin
        Sequel.tsk_require "jdbc/#{name}"
      rescue LoadError
        # jdbc gem not used, hopefully the user has the .jar in their CLASSPATH
      end
    end

    # JDBC Databases offer a fairly uniform interface that does not change
    # much based on the sub adapter.
    class Database < Sequel::Database
      set_adapter_scheme :jdbc
      
      # The type of database we are connecting to
      attr_reader :database_type
      
      # The Java database driver we are using
      attr_reader :driver
      
      # Whether to convert some Java types to ruby types when retrieving rows.
      # True by default, can be set to false to roughly double performance when
      # fetching rows.
      attr_accessor :convert_types
      
      # Call the DATABASE_SETUP proc directly after initialization,
      # so the object always uses sub adapter specific code.  Also,
      # raise an error immediately if the connection doesn't have a
      # uri, since JDBC requires one.
      def initialize(opts)
        super
        @convert_types = typecast_value_boolean(@opts.fetch(:convert_types, true))
        raise(Error, "No connection string specified") unless uri
        
        resolved_uri = jndi? ? get_uri_from_jndi : uri

        if match = /\Ajdbc:([^:]+)/.match(resolved_uri) and prok = DATABASE_SETUP[match[1].to_sym]
          @driver = prok.call(self)
        end        
      end
      
      # Execute the given stored procedure with the give name. If a block is
      # given, the stored procedure should return rows.
      def call_sproc(name, opts = {})
        args = opts[:args] || []
        sql = "{call #{name}(#{args.map{'?'}.join(',')})}"
        synchronize(opts[:server]) do |conn|
          cps = conn.prepareCall(sql)

          i = 0
          args.each{|arg| set_ps_arg(cps, arg, i+=1)}

          begin
            if block_given?
              yield log_yield(sql){cps.executeQuery}
            else
              case opts[:type]
              when :insert
                log_yield(sql){cps.executeUpdate}
                last_insert_id(conn, opts)
              else
                log_yield(sql){cps.executeUpdate}
              end
            end
          rescue NativeException, JavaSQL::SQLException => e
            raise_error(e)
          ensure
            cps.close
          end
        end
      end
         
      # Connect to the database using JavaSQL::DriverManager.getConnection.
      def connect(server)
        opts = server_opts(server)
        conn = if jndi?
          get_connection_from_jndi
        else
          args = [uri(opts)]
          args.concat([opts[:user], opts[:password]]) if opts[:user] && opts[:password]
          begin
            JavaSQL::DriverManager.getConnection(*args)
          rescue => e
            raise e unless driver
            # If the DriverManager can't get the connection - use the connect
            # method of the driver. (This happens under Tomcat for instance)
            props = java.util.Properties.new
            if opts && opts[:user] && opts[:password]
              props.setProperty("user", opts[:user])
              props.setProperty("password", opts[:password])
            end
            driver.new.connect(args[0], props) rescue (raise e)
          end
        end
        setup_connection(conn)
      end
      
      # Return instances of JDBC::Dataset with the given opts.
      def dataset(opts = nil)
        JDBC::Dataset.new(self, opts)
      end
      
      # Execute the given SQL.  If a block is given, if should be a SELECT
      # statement or something else that returns rows.
      def execute(sql, opts={}, &block)
        return call_sproc(sql, opts, &block) if opts[:sproc]
        return execute_prepared_statement(sql, opts, &block) if [Symbol, Dataset].any?{|c| sql.is_a?(c)}
        synchronize(opts[:server]) do |conn|
          statement(conn) do |stmt|
            if block_given?
              yield log_yield(sql){stmt.executeQuery(sql)}
            else
              case opts[:type]
              when :ddl
                log_yield(sql){stmt.execute(sql)}
              when :insert
                log_yield(sql) do
                  if requires_return_generated_keys?
                    stmt.executeUpdate(sql, JavaSQL::Statement.RETURN_GENERATED_KEYS)
                  else
                    stmt.executeUpdate(sql)
                  end
                end
                last_insert_id(conn, opts.merge(:stmt=>stmt))
              else
                log_yield(sql){stmt.executeUpdate(sql)}
              end
            end
          end
        end
      end
      alias execute_dui execute
      
      # Execute the given DDL SQL, which should not return any
      # values or rows.
      def execute_ddl(sql, opts={})
        execute(sql, {:type=>:ddl}.merge(opts))
      end
      
      # Execute the given INSERT SQL, returning the last inserted
      # row id.
      def execute_insert(sql, opts={})
        execute(sql, {:type=>:insert}.merge(opts))
      end
      
      # Use the JDBC metadata to get the index information for the table.
      def indexes(table, opts={})
        m = output_identifier_meth
        im = input_identifier_meth
        schema, table = schema_and_table(table)
        schema ||= opts[:schema]
        schema = im.call(schema) if schema
        table = im.call(table)
        indexes = {}
        metadata(:getIndexInfo, nil, schema, table, false, true) do |r|
          next unless name = r[:column_name]
          next if respond_to?(:primary_key_index_re, true) and r[:index_name] =~ primary_key_index_re 
          i = indexes[m.call(r[:index_name])] ||= {:columns=>[], :unique=>[false, 0].include?(r[:non_unique])}
          i[:columns] << m.call(name)
        end
        indexes
      end 

      # All tables in this database
      def tables(opts={})
        ts = []
        m = output_identifier_meth
        metadata(:getTables, nil, nil, nil, ['TABLE'].to_java(:string)){|h| ts << m.call(h[:table_name])}
        ts
      end
      
      # The uri for this connection.  You can specify the uri
      # using the :uri, :url, or :database options.  You don't
      # need to worry about this if you use Sequel.connect
      # with the JDBC connectrion strings.
      def uri(opts={})
        opts = @opts.merge(opts)
        ur = opts[:uri] || opts[:url] || opts[:database]
        ur =~ /^\Ajdbc:/ ? ur : "jdbc:#{ur}"
      end

      # Whether or not JNDI is being used for this connection.
      def jndi?
        !!(uri =~ JNDI_URI_REGEXP)
      end
      
      private
         
      # Close given adapter connections
      def disconnect_connection(c)
        c.close
      end
      
      # Execute the prepared statement.  If the provided name is a
      # dataset, use that as the prepared statement, otherwise use
      # it as a key to look it up in the prepared_statements hash.
      # If the connection we are using has already prepared an identical
      # statement, use that statement instead of creating another.
      # Otherwise, prepare a new statement for the connection, bind the
      # variables, and execute it.
      def execute_prepared_statement(name, opts={})
        args = opts[:arguments]
        if Dataset === name
          ps = name
          name = ps.prepared_statement_name
        else
          ps = prepared_statements[name]
        end
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          if name and cps = conn.prepared_statements[name] and cps[0] == sql
            cps = cps[1]
          else
            log_yield("Closing #{name}"){cps[1].close} if cps
            cps = log_yield("Preparing#{" #{name}:" if name} #{sql}"){conn.prepareStatement(sql)}
            conn.prepared_statements[name] = [sql, cps] if name
          end
          i = 0
          args.each{|arg| set_ps_arg(cps, arg, i+=1)}
          msg = "Executing#{" #{name}" if name}"
          begin
            if block_given?
              yield log_yield(msg, args){cps.executeQuery}
            else
              case opts[:type]
              when :ddl
                log_yield(msg, args){cps.execute}
              when :insert
                log_yield(msg, args){cps.executeUpdate}
                last_insert_id(conn, opts.merge(:prepared=>true))
              else
                log_yield(msg, args){cps.executeUpdate}
              end
            end
          rescue NativeException, JavaSQL::SQLException => e
            raise_error(e)
          ensure
            cps.close unless name
          end
        end
      end

      # Gets the JDBC connection uri from the JNDI resource.
      def get_uri_from_jndi
        conn = get_connection_from_jndi
        conn.meta_data.url
      ensure
        conn.close if conn
      end
      
      # Gets the connection from JNDI.
      def get_connection_from_jndi
        jndi_name = JNDI_URI_REGEXP.match(uri)[1]
        JavaxNaming::InitialContext.new.lookup(jndi_name).connection
      end
            
      # Support fractional seconds for Time objects used in bound variables
      def java_sql_timestamp(time)
        millis = time.to_i * 1000
        ts = java.sql.Timestamp.new(millis)
        ts.setNanos(time.usec * 1000)
        ts
      end

      # Log the given SQL and then execute it on the connection, used by
      # the transaction code.
      def log_connection_execute(conn, sql)
        statement(conn){|s| log_yield(sql){s.execute(sql)}}
      end

      # By default, there is no support for determining the last inserted
      # id, so return nil.  This method should be overridden in
      # sub adapters.
      def last_insert_id(conn, opts)
        nil
      end
      
      # Yield the metadata for this database
      def metadata(*args, &block)
        synchronize do |c|
          result = c.getMetaData.send(*args)
          begin
            metadata_dataset.send(:process_result_set, result, &block)
          ensure
            result.close
          end
        end
      end

      # Treat SQLExceptions with a "Connection Error" SQLState as disconnects
      def raise_error(exception, opts={})
        cause = exception.respond_to?(:cause) ? exception.cause : exception
        super(exception, {:disconnect => cause.respond_to?(:getSQLState) && cause.getSQLState =~ /^08/}.merge(opts))
      end

      # Java being java, you need to specify the type of each argument
      # for the prepared statement, and bind it individually.  This
      # guesses which JDBC method to use, and hopefully JRuby will convert
      # things properly for us.
      def set_ps_arg(cps, arg, i)
        case arg
        when Integer
          cps.setLong(i, arg)
        when Sequel::SQL::Blob
          cps.setBytes(i, arg.to_java_bytes)
        when String
          cps.setString(i, arg)
        when Date, Java::JavaSql::Date
          cps.setDate(i, arg)
        when DateTime, Java::JavaSql::Timestamp
          cps.setTimestamp(i, arg)
        when Time
          cps.setTimestamp(i, java_sql_timestamp(arg))
        when Float
          cps.setDouble(i, arg)
        when TrueClass, FalseClass
          cps.setBoolean(i, arg)
        when nil
          cps.setNull(i, JavaSQL::Types::NULL)
        else
          cps.setObject(i, arg)
        end
      end
      
      # Add a prepared_statements accessor to the connection,
      # and set it to an empty hash.  This is used to store
      # adapter specific prepared statements.
      def setup_connection(conn)
        class << conn
          attr_accessor :prepared_statements
        end
        conn.prepared_statements = {}
        conn
      end
      
      # Parse the table schema for the given table.
      def schema_parse_table(table, opts={})
        m = output_identifier_meth
        im = input_identifier_meth
        ds = dataset
        schema, table = schema_and_table(table)
        schema ||= opts[:schema]
        schema = im.call(schema) if schema
        table = im.call(table)
        pks, ts = [], []
        metadata(:getPrimaryKeys, nil, schema, table) do |h|
          pks << h[:column_name]
        end
        metadata(:getColumns, nil, schema, table, nil) do |h|
          s = {:type=>schema_column_type(h[:type_name]), :db_type=>h[:type_name], :default=>(h[:column_def] == '' ? nil : h[:column_def]), :allow_null=>(h[:nullable] != 0), :primary_key=>pks.include?(h[:column_name]), :column_size=>h[:column_size], :scale=>h[:decimal_digits]}
          if s[:db_type] =~ DECIMAL_TYPE_RE && s[:scale] == 0
            s[:type] = :integer
          end
          ts << [m.call(h[:column_name]), s]
        end
        ts
      end
      
      # Yield a new statement object, and ensure that it is closed before returning.
      def statement(conn)
        stmt = conn.createStatement
        yield stmt
      rescue NativeException, JavaSQL::SQLException => e
        raise_error(e)
      ensure
        stmt.close if stmt
      end

      # This method determines whether or not to add
      # Statement.RETURN_GENERATED_KEYS as an argument when inserting rows.
      # Sub-adapters that require this should override this method.
      def requires_return_generated_keys?
        false
      end
    end
    
    class Dataset < Sequel::Dataset
      include StoredProcedures
      
      # Use JDBC PreparedStatements instead of emulated ones.  Statements
      # created using #prepare are cached at the connection level to allow
      # reuse.  This also supports bind variables by using unnamed
      # prepared statements created using #call.
      module PreparedStatementMethods
        include Sequel::Dataset::UnnumberedArgumentMapper
        
        private
        
        # Execute the prepared SQL using the stored type and
        # arguments derived from the hash passed to call.
        def execute(sql, opts={}, &block)
          super(self, {:arguments=>bind_arguments, :type=>sql_query_type}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
          super(self, {:arguments=>bind_arguments, :type=>sql_query_type}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_insert(sql, opts={}, &block)
          super(self, {:arguments=>bind_arguments, :type=>sql_query_type}.merge(opts), &block)
        end
      end
      
      # Use JDBC CallableStatements to execute stored procedures.  Only supported
      # if the underlying database has stored procedure support.
      module StoredProcedureMethods
        include Sequel::Dataset::StoredProcedureMethods
        
        private
        
        # Execute the database stored procedure with the stored arguments.
        def execute(sql, opts={}, &block)
          super(@sproc_name, {:args=>@sproc_args, :sproc=>true, :type=>sql_query_type}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts={}, &block)
          super(@sproc_name, {:args=>@sproc_args, :sproc=>true, :type=>sql_query_type}.merge(opts), &block)
        end
        
        # Same as execute, explicit due to intricacies of alias and super.
        def execute_insert(sql, opts={}, &block)
          super(@sproc_name, {:args=>@sproc_args, :sproc=>true, :type=>sql_query_type}.merge(opts), &block)
        end
      end
      
      # Whether to convert some Java types to ruby types when retrieving rows.
      # Uses the database's setting by default, can be set to false to roughly
      # double performance when fetching rows.
      attr_accessor :convert_types
      
      # Use the convert_types default setting from the database
      def initialize(db, opts={})
        @convert_types = db.convert_types
        super
      end
      
      # Correctly return rows from the database and return them as hashes.
      def fetch_rows(sql, &block)
        execute(sql){|result| process_result_set(result, &block)}
        self
      end
      
      # Create a named prepared statement that is stored in the
      # database (and connection) for reuse.
      def prepare(type, name=nil, *values)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        if name
          ps.prepared_statement_name = name
          db.prepared_statements[name] = ps
        end
        ps
      end
      
      private
      
      # Convert the type.  Used for converting Java types to ruby types.
      def convert_type(v)
        case v
        when Java::JavaSQL::Timestamp, Java::JavaSQL::Time
          Sequel.database_to_application_timestamp(v.to_string)
        when Java::JavaSQL::Date
          Sequel.string_to_date(v.to_string)
        when Java::JavaIo::BufferedReader
          lines = []
          while(line = v.read_line) do lines << line end
          lines.join("\n")
        when Java::JavaMath::BigDecimal
          BigDecimal.new(v.to_string)
        when Java::byte[]
          Sequel::SQL::Blob.new(String.from_java_bytes(v))
        when Java::JavaSQL::Blob
          convert_type(v.getBytes(1, v.length))
        else
          v
        end
      end
      
      # Extend the dataset with the JDBC stored procedure methods.
      def prepare_extend_sproc(ds)
        ds.extend(StoredProcedureMethods)
      end
      
      # Split out from fetch rows to allow processing of JDBC result sets
      # that don't come from issuing an SQL string.
      def process_result_set(result)
        # get column names
        meta = result.getMetaData
        cols = []
        i = 0
        meta.getColumnCount.times{cols << [output_identifier(meta.getColumnLabel(i+=1)), i]}
        @columns = cols.map{|c| c.at(0)}
        blk = result_set_object_getter
        # get rows
        while result.next
          row = {}
          cols.each do |n, i|
            row[n] = blk.call(result, n, i)
          end
          yield row
        end
      end

      def result_set_object_getter
        if @convert_types
          lambda {|result, n, i| convert_type(result.getObject(i))}
        else
          lambda {|result, n, i| result.getObject(i)}
        end
      end
    end
  end
end
