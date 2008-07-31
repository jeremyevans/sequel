require 'java'

module Sequel
  # Houses Sequel's JDBC support when running on JRuby.
  # Support for individual database types is done using sub adapters.
  # PostgreSQL, MySQL, SQLite, and MSSQL all have relatively good support,
  # close the the level supported by the native adapter.
  # PostgreSQL, MySQL, SQLite can load necessary support using
  # the jdbc-* gem, if it is installed, though they will work if you
  # have the correct .jar in your CLASSPATH.  Oracle and MSSQL should
  # load the necessary support if you have the .jar in your CLASSPATH.
  # For all other databases, the Java class should be loaded manually
  # before calling Sequel.connect.
  #
  # Note that when using a JDBC adapter, the best way to use Sequel
  # is via Sequel.connect, NOT Sequel.jdbc.  Use the JDBC connection
  # string when connecting, which will be in a different format than
  # the native connection string.  The connection string should start
  # with 'jdbc:'.  For PostgreSQL, use 'jdbc:postgresql:', and for
  # SQLite you do not need 2 preceding slashes for the database name
  # (use no preceding slashes for a relative path, and one preceding
  # slash for an absolute path).
  module JDBC
    # Make it accesing the java.lang hierarchy more ruby friendly.
    module JavaLang
      include_package 'java.lang'
    end
    
    # Make it accesing the java.sql hierarchy more ruby friendly.
    module JavaSQL
      include_package 'java.sql'
    end
    
    # Contains procs keyed on sub adapter type that extend the
    # given database object so it supports the correct database type.
    DATABASE_SETUP = {:postgresql=>proc do |db|
        require 'sequel_core/adapters/jdbc/postgresql'
        db.extend(Sequel::JDBC::Postgres::DatabaseMethods)
        JDBC.load_gem('postgres')
        org.postgresql.Driver
      end,
      :mysql=>proc do |db|
        require 'sequel_core/adapters/jdbc/mysql'
        db.extend(Sequel::JDBC::MySQL::DatabaseMethods)
        JDBC.load_gem('mysql')
        com.mysql.jdbc.Driver
      end,
      :sqlite=>proc do |db|
        require 'sequel_core/adapters/jdbc/sqlite'
        db.extend(Sequel::JDBC::SQLite::DatabaseMethods)
        JDBC.load_gem('sqlite3')
        org.sqlite.JDBC
      end,
      :oracle=>proc{oracle.jdbc.driver.OracleDriver},
      :sqlserver=>proc do |db|
        require 'sequel_core/adapters/shared/mssql'
        db.extend(Sequel::MSSQL::DatabaseMethods)
        com.microsoft.sqlserver.jdbc.SQLServerDriver
      end
    }
    
    # Allowing loading the necessary JDBC support via a gem, which
    # works for PostgreSQL, MySQL, and SQLite.
    def self.load_gem(name)
      begin
        require "jdbc/#{name}"
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
      
      # Call the DATABASE_SETUP proc directly after initialization,
      # so the object always uses sub adapter specific code.  Also,
      # raise an error immediately if the connection doesn't have a
      # uri, since JDBC requires one.
      def initialize(opts)
        super(opts)
        raise(Error, "No connection string specified") unless uri
        if match = /\Ajdbc:([^:]+)/.match(uri) and prok = DATABASE_SETUP[match[1].to_sym]
          prok.call(self)
        end
      end
      
      # Connect to the database using JavaSQL::DriverManager.getConnection.
      def connect
        setup_connection(JavaSQL::DriverManager.getConnection(uri))
      end
      
      # Return instances of JDBC::Dataset with the given opts.
      def dataset(opts = nil)
        JDBC::Dataset.new(self, opts)
      end
      
      # Close all adapter connections
      def disconnect
        @pool.disconnect {|c| c.close}
      end
      
      # Execute the given SQL, which should be a SELECT statement
      # or something else that returns rows.
      def execute(sql, &block)
        _execute(sql, :type=>:select, &block)
      end
      
      # Execute the given DDL SQL, which should not return any
      # values or rows.
      def execute_ddl(sql)
        _execute(sql, :type=>:ddl)
      end
      
      # Execute the given DELETE, UPDATE, or INSERT SQL, returning
      # the number of rows affected.
      def execute_dui(sql)
        _execute(sql, :type=>:dui)
      end
      
      # Execute the given INSERT SQL, returning the last inserted
      # row id.
      def execute_insert(sql)
        _execute(sql, :type=>:insert)
      end
      
      # Execute the prepared statement.  If the provided name is a
      # dataset, use that as the prepared statement, otherwise use
      # it as a key to look it up in the prepared_statements hash.
      # If the connection we are using has already prepared an identical
      # statement, use that statement instead of creating another.
      # Otherwise, prepare a new statement for the connection, bind the
      # variables, and execute it.
      def execute_prepared_statement(name, args=[], opts={})
        if Dataset === name
          ps = name
          name = ps.prepared_statement_name
        else
          ps = prepared_statements[name]
        end
        sql = ps.prepared_sql
        synchronize do |conn|
          if name and cps = conn.prepared_statements[name] and cps[0] == sql
            cps = cps[1]
          else
            if cps
              log_info("Closing #{name}")
              cps[1].close
            end
            log_info("Preparing#{" #{name}:" if name} #{sql}")
            cps = conn.prepareStatement(sql)
            conn.prepared_statements[name] = [sql, cps] if name
          end
          i = 0
          args.each{|arg| set_ps_arg(cps, arg, i+=1)}
          log_info("Executing#{" #{name}" if name}", args)
          begin
            case opts[:type]
            when :select
              yield cps.executeQuery
            when :ddl
              cps.execute
            when :insert
              cps.executeUpdate
              last_insert_id(conn, opts)
            else
              cps.executeUpdate
            end
          rescue NativeException, JavaSQL::SQLException => e
            raise Error, e.message
          ensure
            cps.close unless name
          end
        end
      end
      
      # Default transaction method that should work on most JDBC
      # databases.  Does not use the JDBC transaction methods, uses
      # SQL BEGIN/ROLLBACK/COMMIT statements instead.
      def transaction
        synchronize do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
          stmt = conn.createStatement
          begin
            log_info(Sequel::Database::SQL_BEGIN)
            stmt.execute(Sequel::Database::SQL_BEGIN)
            @transactions << Thread.current
            yield(conn)
          rescue Exception => e
            log_info(Sequel::Database::SQL_ROLLBACK)
            stmt.execute(Sequel::Database::SQL_ROLLBACK)
            raise e unless Error::Rollback === e
          ensure
            unless e
              log_info(Sequel::Database::SQL_COMMIT)
              stmt.execute(Sequel::Database::SQL_COMMIT)
            end
            stmt.close
            @transactions.delete(Thread.current)
          end
        end
      end
      
      # The uri for this connection.  You can specify the uri
      # using the :uri, :url, or :database options.  You don't
      # need to worry about this if you use Sequel.connect
      # with the JDBC connectrion strings.
      def uri
        ur = @opts[:uri] || @opts[:url] || @opts[:database]
        ur =~ /^\Ajdbc:/ ? ur : "jdbc:#{ur}"
      end
      alias url uri
      
      private
      
      # Execute the SQL.  Use the :type option to see which JDBC method
      # to use.
      def _execute(sql, opts)
        log_info(sql)
        synchronize do |conn|
          stmt = conn.createStatement
          begin
            case opts[:type]
            when :select
              yield stmt.executeQuery(sql)
            when :ddl
              stmt.execute(sql)
            when :insert
              stmt.executeUpdate(sql)
              last_insert_id(conn, opts)
            else
              stmt.executeUpdate(sql)
            end
          rescue NativeException, JavaSQL::SQLException => e
            raise Error, e.message
          ensure
            stmt.close
          end
        end
      end
      
      # By default, there is no support for determining the last inserted
      # id, so return nil.  This method should be overridden in
      # sub adapters.
      def last_insert_id(conn, opts)
        nil
      end
      
      # Java being java, you need to specify the type of each argument
      # for the prepared statement, and bind it individually.  This
      # guesses which JDBC method to use, and hopefully JRuby will convert
      # things properly for us.
      def set_ps_arg(cps, arg, i)
        case arg
        when Integer
          cps.setInt(i, arg)
        when String
          cps.setString(i, arg)
        when Date
          cps.setDate(i, arg)
        when Time, DateTime, Java::JavaSql::Timestamp
          cps.setTimestamp(i, arg)
        when Float
          cps.setDouble(i, arg)
        when nil
          cps.setNull(i, JavaSQL::Types::NULL)
        end
      end
      
      # Add a prepared_statements accessor to the connection,
      # and set it to an empty hash.  This is used to store
      # adapter specific prepared statements.
      def setup_connection(conn)
        conn.meta_eval{attr_accessor :prepared_statements}
        conn.prepared_statements = {}
        conn
      end
      
      # The JDBC adapter should not need the pool to convert exceptions.
      def connection_pool_default_options
        super.merge(:pool_convert_exceptions=>false)
      end
    end
    
    class Dataset < Sequel::Dataset
      # Use JDBC PreparedStatements instead of emulated ones.  Statements
      # created using #prepare are cached at the connection level to allow
      # reuse.  This also supports bind variables by using unnamed
      # prepared statements created using #call.
      module PreparedStatementMethods
        include Sequel::Dataset::UnnumberedArgumentMapper
        
        private
        
        # Execute the prepared SQL using the stored type and
        # arguments derived from the hash passed to call.
        def execute(sql, &block)
          @db.execute_prepared_statement(self, bind_arguments, :type=>sql_query_type, &block)
        end
        alias execute_dui execute
        alias execute_insert execute
      end
      
      # Create an unnamed prepared statement and call it.  Allows the
      # use of bind variables.
      def call(type, hash, values=nil, &block)
        prepare(type, nil, values).call(hash, &block)
      end
      
      # Correctly return rows from the database and return them as hashes.
      def fetch_rows(sql, &block)
        @db.synchronize do
          execute(sql) do |result|
            # get column names
            meta = result.getMetaData
            column_count = meta.getColumnCount
            @columns = []
            column_count.times {|i| @columns << meta.getColumnName(i+1).to_sym}

            # get rows
            while result.next
              row = {}
              @columns.each_with_index {|v, i| row[v] = result.getObject(i+1)}
              yield row
            end
          end
        end
        self
      end
      
      # Use the ISO values for dates and times.
      def literal(v)
        case v
        when Time
          literal(v.iso8601)
        when Date, DateTime, Java::JavaSql::Timestamp
          literal(v.to_s)
        else
          super
        end
      end
      
      # Create a named prepared statement that is stored in the
      # database (and connection) for reuse.
      def prepare(type, name, values=nil)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        if name
          ps.prepared_statement_name = name
          db.prepared_statements[name] = ps
        end
        ps
      end
    end
  end
end

class Java::JavaSQL::Timestamp
  # Add a usec method in order to emulate Time values.
  def usec
    getNanos/1000
  end
end
