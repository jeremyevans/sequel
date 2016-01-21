# frozen-string-literal: true

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
    
    # Contains procs keyed on subadapter type that extend the
    # given database object so it supports the correct database type.
    DATABASE_SETUP = {}
    
    # Allow loading the necessary JDBC support via a gem.
    def self.load_gem(name)
      begin
        require "jdbc/#{name.to_s.downcase}"
      rescue LoadError
        # jdbc gem not used, hopefully the user has the .jar in their CLASSPATH
      else
        if defined?(::Jdbc) && ( ::Jdbc.const_defined?(name) rescue nil )
          jdbc_module = ::Jdbc.const_get(name) # e.g. Jdbc::SQLite3
          jdbc_module.load_driver if jdbc_module.respond_to?(:load_driver)
        end
      end
    end

    # Attempt to load the JDBC driver class, which should be specified as a string
    # containing the driver class name (which JRuby should autoload).
    # Note that the string is evaled, so this method is not safe to call with
    # untrusted input.
    # Raise a Sequel::AdapterNotFound if evaluating the class name raises a NameError.
    def self.load_driver(drv, gem=nil)
      load_gem(gem) if gem
      eval drv
    rescue NameError
      raise Sequel::AdapterNotFound, "#{drv} not loaded#{", try installing jdbc-#{gem.to_s.downcase} gem" if gem}"
    end

    class TypeConvertor
      %w'Boolean Float Double Int Long Short'.each do |meth|
        class_eval("def #{meth}(r, i) v = r.get#{meth}(i); v unless r.wasNull end", __FILE__, __LINE__)
      end
      %w'Object Array String Time Date Timestamp BigDecimal Blob Bytes Clob'.each do |meth|
        class_eval("def #{meth}(r, i) r.get#{meth}(i) end", __FILE__, __LINE__)
      end
      def RubyTime(r, i)
        if v = r.getTime(i)
          Sequel.string_to_time("#{v.to_string}.#{sprintf('%03i', v.getTime.divmod(1000).last)}")
        end
      end
      def RubyDate(r, i)
        if v = r.getDate(i)
          Date.civil(v.getYear + 1900, v.getMonth + 1, v.getDate)
        end
      end
      def RubyTimestamp(r, i)
        if v = r.getTimestamp(i)
          Sequel.database_to_application_timestamp([v.getYear + 1900, v.getMonth + 1, v.getDate, v.getHours, v.getMinutes, v.getSeconds, v.getNanos])
        end
      end
      def RubyBigDecimal(r, i)
        if v = r.getBigDecimal(i)
          BigDecimal.new(v.to_string)
        end
      end
      def RubyBlob(r, i)
        if v = r.getBytes(i)
          Sequel::SQL::Blob.new(String.from_java_bytes(v))
        end
      end
      def RubyClob(r, i)
        if v = r.getClob(i)
          v.getSubString(1, v.length)
        end
      end

      INSTANCE = new
      o = INSTANCE
      MAP = Hash.new(o.method(:Object))
      types = Java::JavaSQL::Types

      {
        :ARRAY => :Array,
        :BOOLEAN => :Boolean,
        :CHAR => :String,
        :DOUBLE => :Double,
        :FLOAT => :Double,
        :INTEGER => :Int,
        :LONGNVARCHAR => :String,
        :LONGVARCHAR => :String,
        :NCHAR => :String,
        :REAL => :Float,
        :SMALLINT => :Short,
        :TINYINT => :Short,
        :VARCHAR => :String,
      }.each do |type, meth|
        MAP[types.const_get(type)] = o.method(meth) 
      end
      BASIC_MAP = MAP.dup

      {
        :BINARY => :Blob,
        :BLOB => :Blob,
        :CLOB => :Clob,
        :DATE => :Date,
        :DECIMAL => :BigDecimal,
        :LONGVARBINARY => :Blob,
        :NCLOB => :Clob,
        :NUMERIC => :BigDecimal,
        :TIME => :Time,
        :TIMESTAMP => :Timestamp,
        :VARBINARY => :Blob,
      }.each do |type, meth|
        BASIC_MAP[types.const_get(type)] = o.method(meth) 
        MAP[types.const_get(type)] = o.method(:"Ruby#{meth}") 
      end
    end

    # JDBC Databases offer a fairly uniform interface that does not change
    # much based on the sub adapter.
    class Database < Sequel::Database
      set_adapter_scheme :jdbc
      
      # The type of database we are connecting to
      attr_reader :database_type
      
      # The Java database driver we are using (should be a Java class)
      attr_reader :driver
      
      # Whether to convert some Java types to ruby types when retrieving rows.
      # True by default, can be set to false to roughly double performance when
      # fetching rows.
      attr_accessor :convert_types

      # The fetch size to use for JDBC Statement objects created by this database.
      # By default, this is nil so a fetch size is not set explicitly.
      attr_accessor :fetch_size

      # Map of JDBC type ids to callable objects that return appropriate ruby values.
      attr_reader :type_convertor_map

      # Map of JDBC type ids to callable objects that return appropriate ruby or java values.
      attr_reader :basic_type_convertor_map

      # Execute the given stored procedure with the give name. If a block is
      # given, the stored procedure should return rows.
      def call_sproc(name, opts = OPTS)
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
            JavaSQL::DriverManager.setLoginTimeout(opts[:login_timeout]) if opts[:login_timeout]
            raise StandardError, "skipping regular connection" if opts[:jdbc_properties]
            JavaSQL::DriverManager.getConnection(*args)
          rescue JavaSQL::SQLException, NativeException, StandardError => e
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
              c = driver.new.connect(args[0], props)
              raise(Sequel::DatabaseError, 'driver.new.connect returned nil: probably bad JDBC connection string') unless c
              c
            rescue JavaSQL::SQLException, NativeException, StandardError => e2
              unless e2.message == e.message
                e2.message << "\n#{e.class.name}: #{e.message}"
              end
              raise e2
            end
          end
        end
        setup_connection(conn)
      end

      # Close given adapter connections, and delete any related prepared statements.
      def disconnect_connection(c)
        @connection_prepared_statements_mutex.synchronize{@connection_prepared_statements.delete(c)}
        c.close
      end
      
      # Execute the given SQL.  If a block is given, if should be a SELECT
      # statement or something else that returns rows.
      def execute(sql, opts=OPTS, &block)
        return call_sproc(sql, opts, &block) if opts[:sproc]
        return execute_prepared_statement(sql, opts, &block) if [Symbol, Dataset].any?{|c| sql.is_a?(c)}
        synchronize(opts[:server]) do |conn|
          statement(conn) do |stmt|
            if block
              if size = fetch_size
                stmt.setFetchSize(size)
              end
              yield log_yield(sql){stmt.executeQuery(sql)}
            else
              case opts[:type]
              when :ddl
                log_yield(sql){stmt.execute(sql)}
              when :insert
                log_yield(sql){execute_statement_insert(stmt, sql)}
                last_insert_id(conn, Hash[opts].merge!(:stmt=>stmt))
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
      def execute_ddl(sql, opts=OPTS)
        opts = Hash[opts]
        opts[:type] = :ddl
        execute(sql, opts)
      end
      
      # Execute the given INSERT SQL, returning the last inserted
      # row id.
      def execute_insert(sql, opts=OPTS)
        opts = Hash[opts]
        opts[:type] = :insert
        execute(sql, opts)
      end

      # Use the JDBC metadata to get a list of foreign keys for the table.
      def foreign_key_list(table, opts=OPTS)
        m = output_identifier_meth
        schema, table = metadata_schema_and_table(table, opts)
        foreign_keys = {}
        metadata(:getImportedKeys, nil, schema, table) do |r|
          if fk = foreign_keys[r[:fk_name]]
            fk[:columns] << [r[:key_seq], m.call(r[:fkcolumn_name])]
            fk[:key] << [r[:key_seq], m.call(r[:pkcolumn_name])]
          elsif r[:fk_name]
            foreign_keys[r[:fk_name]] = {:name=>m.call(r[:fk_name]), :columns=>[[r[:key_seq], m.call(r[:fkcolumn_name])]], :table=>m.call(r[:pktable_name]), :key=>[[r[:key_seq], m.call(r[:pkcolumn_name])]]}
          end
        end
        foreign_keys.values.each do |fk|
          [:columns, :key].each do |k|
            fk[k] = fk[k].sort.map{|_, v| v}
          end
        end
      end

      # Use the JDBC metadata to get the index information for the table.
      def indexes(table, opts=OPTS)
        m = output_identifier_meth
        schema, table = metadata_schema_and_table(table, opts)
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
      def tables(opts=OPTS)
        get_tables('TABLE', opts)
      end
      
      # The uri for this connection.  You can specify the uri
      # using the :uri, :url, or :database options.  You don't
      # need to worry about this if you use Sequel.connect
      # with the JDBC connectrion strings.
      def uri(opts=OPTS)
        opts = @opts.merge(opts)
        ur = opts[:uri] || opts[:url] || opts[:database]
        ur =~ /^\Ajdbc:/ ? ur : "jdbc:#{ur}"
      end

      # All views in this database
      def views(opts=OPTS)
        get_tables('VIEW', opts)
      end

      private
         
      # Call the DATABASE_SETUP proc directly after initialization,
      # so the object always uses sub adapter specific code.  Also,
      # raise an error immediately if the connection doesn't have a
      # uri, since JDBC requires one.
      def adapter_initialize
        @connection_prepared_statements = {}
        @connection_prepared_statements_mutex = Mutex.new
        @fetch_size = @opts[:fetch_size] ? typecast_value_integer(@opts[:fetch_size]) : default_fetch_size
        @convert_types = typecast_value_boolean(@opts.fetch(:convert_types, true))
        raise(Error, "No connection string specified") unless uri
        
        resolved_uri = jndi? ? get_uri_from_jndi : uri
        setup_type_convertor_map_early

        @driver = if (match = /\Ajdbc:([^:]+)/.match(resolved_uri)) && (prok = Sequel::Database.load_adapter(match[1].to_sym, :map=>DATABASE_SETUP, :subdir=>'jdbc'))
          prok.call(self)
        else
          @opts[:driver]
        end        

        setup_type_convertor_map
      end
      
      # Yield the native prepared statements hash for the given connection
      # to the block in a thread-safe manner.
      def cps_sync(conn, &block)
        @connection_prepared_statements_mutex.synchronize{yield(@connection_prepared_statements[conn] ||= {})}
      end

      def database_error_classes
        [NativeException]
      end

      def database_exception_sqlstate(exception, opts)
        if database_exception_use_sqlstates?
          while exception.respond_to?(:cause)
            exception = exception.cause
            return exception.getSQLState if exception.respond_to?(:getSQLState)
          end
        end
        nil
      end

      # Whether the JDBC subadapter should use SQL states for exception handling, true by default.
      def database_exception_use_sqlstates?
        true
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
      def execute_prepared_statement(name, opts=OPTS)
        args = opts[:arguments]
        if name.is_a?(Dataset)
          ps = name
          name = ps.prepared_statement_name
        else
          ps = prepared_statement(name)
        end
        sql = ps.prepared_sql
        synchronize(opts[:server]) do |conn|
          if name and cps = cps_sync(conn){|cpsh| cpsh[name]} and cps[0] == sql
            cps = cps[1]
          else
            log_yield("CLOSE #{name}"){cps[1].close} if cps
            cps = log_yield("PREPARE#{" #{name}:" if name} #{sql}"){prepare_jdbc_statement(conn, sql, opts)}
            if size = fetch_size
              cps.setFetchSize(size)
            end
            cps_sync(conn){|cpsh| cpsh[name] = [sql, cps]} if name
          end
          i = 0
          args.each{|arg| set_ps_arg(cps, arg, i+=1)}
          msg = "EXECUTE#{" #{name}" if name}"
          if ps.log_sql
            msg << " ("
            msg << sql
            msg << ")"
          end
          begin
            if block_given?
              yield log_yield(msg, args){cps.executeQuery}
            else
              case opts[:type]
              when :ddl
                log_yield(msg, args){cps.execute}
              when :insert
                log_yield(msg, args){execute_prepared_statement_insert(cps)}
                last_insert_id(conn, Hash[opts].merge!(:prepared=>true, :stmt=>cps))
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

      # Execute the prepared insert statement
      def execute_prepared_statement_insert(stmt)
        stmt.executeUpdate
      end
      
      # Execute the insert SQL using the statement
      def execute_statement_insert(stmt, sql)
        stmt.executeUpdate(sql)
      end

      # The default fetch size to use for statements.  Nil by default, so that the
      # default for the JDBC driver is used.
      def default_fetch_size
        nil
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
        if schema = opts[:schema]
          schema = schema.to_s
        end
        metadata(:getTables, nil, schema, nil, [type].to_java(:string)){|h| ts << m.call(h[:table_name])}
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
        # Work around jruby 1.6 ruby 1.9 mode bug
        ts.setNanos((RUBY_VERSION >= '1.9.0' && time.nsec != 0) ? time.nsec : time.usec * 1000)
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

      # Return the schema and table suitable for use with metadata queries.
      def metadata_schema_and_table(table, opts)
        im = input_identifier_meth(opts[:dataset])
        schema, table = schema_and_table(table)
        schema ||= opts[:schema]
        schema = im.call(schema) if schema
        table = im.call(table)
        [schema, table]
      end
      
      # Created a JDBC prepared statement on the connection with the given SQL.
      def prepare_jdbc_statement(conn, sql, opts)
        conn.prepareStatement(sql)
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
          set_ps_arg_nil(cps, i)
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

      # Use setString with a nil value by default, but this doesn't work on all subadapters.
      def set_ps_arg_nil(cps, i)
        cps.setString(i, nil)
      end
      
      # Return the connection.  Used to do configuration on the
      # connection object before adding it to the connection pool.
      def setup_connection(conn)
        conn
      end

      def schema_column_set_db_type(schema)
        case schema[:type]
        when :string
          if schema[:db_type] =~ /\A(character( varying)?|n?(var)?char2?)\z/io && schema[:column_size] > 0
            schema[:db_type] += "(#{schema[:column_size]})"
          end
        when :decimal
          if schema[:db_type] =~ /\A(decimal|numeric)\z/io && schema[:column_size] > 0 && schema[:scale] >= 0
            schema[:db_type] += "(#{schema[:column_size]}, #{schema[:scale]})"
          end
        end
      end
      
      # Parse the table schema for the given table.
      def schema_parse_table(table, opts=OPTS)
        m = output_identifier_meth(opts[:dataset])
        schema, table = metadata_schema_and_table(table, opts)
        pks, ts = [], []
        metadata(:getPrimaryKeys, nil, schema, table) do |h|
          next if schema_parse_table_skip?(h, schema)
          pks << h[:column_name]
        end
        schemas = []
        metadata(:getColumns, nil, schema, table, nil) do |h|
          next if schema_parse_table_skip?(h, schema)
          s = {
            :type=>schema_column_type(h[:type_name]),
            :db_type=>h[:type_name],
            :default=>(h[:column_def] == '' ? nil : h[:column_def]),
            :allow_null=>(h[:nullable] != 0),
            :primary_key=>pks.include?(h[:column_name]),
            :column_size=>h[:column_size],
            :scale=>h[:decimal_digits]
          }
          if s[:primary_key]
            s[:auto_increment] = h[:is_autoincrement] == "YES"
          end
          s[:max_length] = s[:column_size] if s[:type] == :string
          if s[:db_type] =~ DECIMAL_TYPE_RE && s[:scale] == 0
            s[:type] = :integer
          end
          schema_column_set_db_type(s)
          schemas << h[:table_schem] unless schemas.include?(h[:table_schem])
          ts << [m.call(h[:column_name]), s]
        end
        if schemas.length > 1
          raise Error, 'Schema parsing in the jdbc adapter resulted in columns being returned for a table with the same name in multiple schemas.  Please explicitly qualify your table with a schema.'
        end
        ts
      end
      
      # Whether schema_parse_table should skip the given row when
      # parsing the schema.
      def schema_parse_table_skip?(h, schema)
        h[:table_schem] == 'INFORMATION_SCHEMA'
      end

      # Called after loading subadapter-specific code, overridable by subadapters.
      def setup_type_convertor_map
      end

      # Called before loading subadapter-specific code, necessary so that subadapter initialization code
      # that runs queries works correctly.  This cannot be overriding in subadapters,
      def setup_type_convertor_map_early
        @type_convertor_map = TypeConvertor::MAP.merge(Java::JavaSQL::Types::TIMESTAMP=>timestamp_convertor)
        @basic_type_convertor_map = TypeConvertor::BASIC_MAP
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

      # A conversion proc for timestamp columns.  This is used to make sure timestamps are converted using the
      # correct timezone.
      def timestamp_convertor
        lambda do |r, i|
          if v = r.getTimestamp(i)
            to_application_timestamp([v.getYear + 1900, v.getMonth + 1, v.getDate, v.getHours, v.getMinutes, v.getSeconds, v.getNanos])
          end
        end
      end
    end
    
    class Dataset < Sequel::Dataset
      include StoredProcedures

      Database::DatasetClass = self
      
      PreparedStatementMethods = prepared_statements_module(
        "sql = self; opts = Hash[opts]; opts[:arguments] = bind_arguments",
        Sequel::Dataset::UnnumberedArgumentMapper,
        %w"execute execute_dui") do
          private

          # Same as execute, explicit due to intricacies of alias and super.
          def execute_insert(sql, opts=OPTS)
            sql = self
            opts = Hash[opts]
            opts[:arguments] = bind_arguments
            opts[:type] = :insert
            super
          end
      end
      
      StoredProcedureMethods = prepared_statements_module(
        "sql = @sproc_name; opts = Hash[opts]; opts[:args] = @sproc_args; opts[:sproc] = true",
        Sequel::Dataset::StoredProcedureMethods,
        %w"execute execute_dui") do
          private

          # Same as execute, explicit due to intricacies of alias and super.
          def execute_insert(sql, opts=OPTS)
            sql = @sproc_name
            opts = Hash[opts]
            opts[:args] = @sproc_args
            opts[:sproc] = true
            opts[:type] = :insert
            super
          end
      end
      
      # Whether to convert some Java types to ruby types when retrieving rows.
      # Uses the database's setting by default, can be set to false to roughly
      # double performance when fetching rows.
      attr_accessor :convert_types

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
          db.set_prepared_statement(name, ps)
        end
        ps
      end

      # Set the fetch size on JDBC ResultSets created from this dataset.
      def with_fetch_size(size)
        clone(:fetch_size=>size)
      end
      
      private

      # Whether we should convert Java types to ruby types for this dataset.
      def convert_types?
        ct = @convert_types
        ct.nil? ? db.convert_types : ct
      end

      # Extend the dataset with the JDBC stored procedure methods.
      def prepare_extend_sproc(ds)
        ds.extend(StoredProcedureMethods)
      end

      # The type conversion proc to use for the given column number i,
      # given the type conversion map and the ResultSetMetaData.
      def type_convertor(map, meta, type, i)
        map[type]
      end

      # The basic type conversion proc to use for the given column number i,
      # given the type conversion map and the ResultSetMetaData.
      #
      # This is implemented as a separate method so that subclasses can
      # override the methods separately.
      def basic_type_convertor(map, meta, type, i)
        map[type]
      end

      # Split out from fetch rows to allow processing of JDBC result sets
      # that don't come from issuing an SQL string.
      def process_result_set(result)
        meta = result.getMetaData
        if fetch_size = opts[:fetch_size]
          result.setFetchSize(fetch_size)
        end
        cols = []
        i = 0
        convert = convert_types?
        map = convert ? db.type_convertor_map : db.basic_type_convertor_map

        meta.getColumnCount.times do
          i += 1
          cols << [output_identifier(meta.getColumnLabel(i)), i, convert ? type_convertor(map, meta, meta.getColumnType(i), i) : basic_type_convertor(map, meta, meta.getColumnType(i), i)]
        end
        @columns = cols.map{|c| c.at(0)}

        while result.next
          row = {}
          cols.each do |n, j, pr|
            row[n] = pr.call(result, j)
          end
          yield row
        end
      ensure
        result.close
      end
    end
  end
end
