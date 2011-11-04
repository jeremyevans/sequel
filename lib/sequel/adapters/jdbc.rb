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
        db.dataset_class = Sequel::JDBC::Postgres::Dataset
        JDBC.load_gem('postgres')
        org.postgresql.Driver
      end,
      :mysql=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/mysql'
        db.extend(Sequel::JDBC::MySQL::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::MySQL::Dataset
        JDBC.load_gem('mysql')
        com.mysql.jdbc.Driver
      end,
      :sqlite=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/sqlite'
        db.extend(Sequel::JDBC::SQLite::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::SQLite::Dataset
        JDBC.load_gem('sqlite3')
        org.sqlite.JDBC
      end,
      :oracle=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/oracle'
        db.extend(Sequel::JDBC::Oracle::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Oracle::Dataset
        Java::oracle.jdbc.driver.OracleDriver
      end,
      :sqlserver=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/sqlserver'
        db.extend(Sequel::JDBC::SQLServer::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::SQLServer::Dataset
        db.send(:set_mssql_unicode_strings)
        com.microsoft.sqlserver.jdbc.SQLServerDriver
      end,
      :jtds=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/jtds'
        db.extend(Sequel::JDBC::JTDS::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::JTDS::Dataset
        db.send(:set_mssql_unicode_strings)
        JDBC.load_gem('jtds')
        Java::net.sourceforge.jtds.jdbc.Driver
      end,
      :h2=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/h2'
        db.extend(Sequel::JDBC::H2::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::H2::Dataset
        JDBC.load_gem('h2')
        org.h2.Driver
      end,
      :hsqldb=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/hsqldb'
        db.extend(Sequel::JDBC::HSQLDB::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::HSQLDB::Dataset
        # Current gem is 1.8.1.3, but Sequel supports 2.2.5
        org.hsqldb.jdbcDriver
      end,
      :derby=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/derby'
        db.extend(Sequel::JDBC::Derby::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Derby::Dataset
        JDBC.load_gem('derby')
        org.apache.derby.jdbc.EmbeddedDriver
      end,
      :as400=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/as400'
        db.extend(Sequel::JDBC::AS400::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::AS400::Dataset
        com.ibm.as400.access.AS400JDBCDriver
      end,
      :"informix-sqli"=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/informix'
        db.extend(Sequel::JDBC::Informix::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Informix::Dataset
        com.informix.jdbc.IfxDriver
      end,
      :db2=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/db2'
        db.extend(Sequel::JDBC::DB2::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::DB2::Dataset
        com.ibm.db2.jcc.DB2Driver
      end,
      :firebirdsql=>proc do |db|
        Sequel.ts_require 'adapters/jdbc/firebird'
        db.extend(Sequel::JDBC::Firebird::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Firebird::Dataset
        org.firebirdsql.jdbc.FBDriver
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
            opts[:jdbc_properties].each{|k,v| props.setProperty(k.to_s, v)} if opts[:jdbc_properties]
            begin
              driver.new.connect(args[0], props)
            rescue => e2
              e.message << "\n#{e2.class.name}: #{e2.message}"
              raise e
            end
          end
        end
        setup_connection(conn)
      end
      
      # Execute the given SQL.  If a block is given, if should be a SELECT
      # statement or something else that returns rows.
      def execute(sql, opts={}, &block)
        return call_sproc(sql, opts, &block) if opts[:sproc]
        return execute_prepared_statement(sql, opts, &block) if [Symbol, Dataset].any?{|c| sql.is_a?(c)}
        synchronize(opts[:server]) do |conn|
          statement(conn) do |stmt|
            if block
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

      # Whether or not JNDI is being used for this connection.
      def jndi?
        !!(uri =~ JNDI_URI_REGEXP)
      end
      
      # All tables in this database
      def tables(opts={})
        get_tables('TABLE', opts)
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

      # All views in this database
      def views(opts={})
        get_tables('VIEW', opts)
      end

      private
         
      # Close given adapter connections
      def disconnect_connection(c)
        c.close
      end
      
      # Raise a disconnect error if the SQL state of the cause of the exception indicates so.
      def disconnect_error?(exception, opts)
        cause = exception.respond_to?(:cause) ? exception.cause : exception
        super || (cause.respond_to?(:getSQLState) && cause.getSQLState =~ /^08/)
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

      # Gets the connection from JNDI.
      def get_connection_from_jndi
        jndi_name = JNDI_URI_REGEXP.match(uri)[1]
        JavaxNaming::InitialContext.new.lookup(jndi_name).connection
      end
            
      # Gets the JDBC connection uri from the JNDI resource.
      def get_uri_from_jndi
        conn = get_connection_from_jndi
        conn.meta_data.url
      ensure
        conn.close if conn
      end
      
      # Backbone of the tables and views support.
      def get_tables(type, opts)
        ts = []
        m = output_identifier_meth
        metadata(:getTables, nil, nil, nil, [type].to_java(:string)){|h| ts << m.call(h[:table_name])}
        ts
      end

      # Support Date objects used in bound variables
      def java_sql_date(date)
        java.sql.Date.new(Time.local(date.year, date.month, date.day).to_i * 1000)
      end

      # Support DateTime objects used in bound variables
      def java_sql_datetime(datetime)
        ts = java.sql.Timestamp.new(Time.local(datetime.year, datetime.month, datetime.day, datetime.hour, datetime.min, datetime.sec).to_i * 1000)
        ts.setNanos((datetime.sec_fraction * (RUBY_VERSION >= '1.9.0' ?  1000000000 : 86400000000000)).to_i)
        ts
      end

      # Support fractional seconds for Time objects used in bound variables
      def java_sql_timestamp(time)
        ts = java.sql.Timestamp.new(time.to_i * 1000)
        ts.setNanos(RUBY_VERSION >= '1.9.0' ? time.nsec : time.usec * 1000)
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
        when Float
          cps.setDouble(i, arg)
        when TrueClass, FalseClass
          cps.setBoolean(i, arg)
        when NilClass
          cps.setString(i, nil)
        when DateTime
          cps.setTimestamp(i, java_sql_datetime(arg))
        when Date
          cps.setDate(i, java_sql_date(arg))
        when Time
          cps.setTimestamp(i, java_sql_timestamp(arg))
        when Java::JavaSql::Timestamp
          cps.setTimestamp(i, arg)
        when Java::JavaSql::Date
          cps.setDate(i, arg)
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
        m = output_identifier_meth(opts[:dataset])
        im = input_identifier_meth(opts[:dataset])
        ds = dataset
        schema, table = schema_and_table(table)
        schema ||= opts[:schema]
        schema = im.call(schema) if schema
        table = im.call(table)
        pks, ts = [], []
        metadata(:getPrimaryKeys, nil, schema, table) do |h|
          next if schema_parse_table_skip?(h, schema)
          pks << h[:column_name]
        end
        metadata(:getColumns, nil, schema, table, nil) do |h|
          next if schema_parse_table_skip?(h, schema)
          s = {:type=>schema_column_type(h[:type_name]), :db_type=>h[:type_name], :default=>(h[:column_def] == '' ? nil : h[:column_def]), :allow_null=>(h[:nullable] != 0), :primary_key=>pks.include?(h[:column_name]), :column_size=>h[:column_size], :scale=>h[:decimal_digits]}
          if s[:db_type] =~ DECIMAL_TYPE_RE && s[:scale] == 0
            s[:type] = :integer
          end
          ts << [m.call(h[:column_name]), s]
        end
        ts
      end
      
      # Whether schema_parse_table should skip the given row when
      # parsing the schema.
      def schema_parse_table_skip?(h, schema)
        h[:table_schem] == 'INFORMATION_SCHEMA'
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

      Database::DatasetClass = self
      
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

      # Use the convert_types default setting from the database.
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

      JAVA_SQL_TIMESTAMP    = Java::JavaSQL::Timestamp
      JAVA_SQL_TIME         = Java::JavaSQL::Time
      JAVA_SQL_DATE         = Java::JavaSQL::Date
      JAVA_SQL_BLOB         = Java::JavaSQL::Blob
      JAVA_SQL_CLOB         = Java::JavaSQL::Clob
      JAVA_BUFFERED_READER  = Java::JavaIo::BufferedReader
      JAVA_BIG_DECIMAL      = Java::JavaMath::BigDecimal
      JAVA_BYTE_ARRAY       = Java::byte[]

      # Convert the type.  Used for converting Java types to ruby types.
      def convert_type(v)
        case v
        when JAVA_SQL_TIMESTAMP
          db.to_application_timestamp([v.getYear + 1900, v.getMonth + 1, v.getDate, v.getHours, v.getMinutes, v.getSeconds, v.getNanos])
        when JAVA_SQL_TIME
          Sequel.string_to_time(v.to_string)
        when JAVA_SQL_DATE
          Sequel.string_to_date(v.to_string)
        when JAVA_BUFFERED_READER
          lines = []
          while(line = v.read_line) do lines << line end
          lines.join("\n")
        when JAVA_BIG_DECIMAL
          BigDecimal.new(v.to_string)
        when JAVA_BYTE_ARRAY
          Sequel::SQL::Blob.new(String.from_java_bytes(v))
        when JAVA_SQL_BLOB
          convert_type(v.getBytes(1, v.length))
        when JAVA_SQL_CLOB
          Sequel::SQL::Blob.new(v.getSubString(1, v.length))
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
        columns = cols.map{|c| c.at(0)}
        if opts[:offset] && offset_returns_row_number_column?
          rn = row_number_column
          columns.delete(rn)
        end
        @columns = columns
        blk = result_set_object_getter
        # get rows
        while result.next
          row = {}
          cols.each do |n, i|
            row[n] = blk.call(result, n, i)
          end
          row.delete(rn) if rn
          yield row
        end
      ensure
        result.close
      end

      # Proc that takes the ResultSet and an integer field number and returns
      # the object for that field.  Respects the @convert_types setting to
      # convert Java types to ruby types.
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
