require 'java'

module Sequel
  module JDBC
    module JavaLang; include_package 'java.lang'; end
    module JavaSQL; include_package 'java.sql'; end
    DATABASE_SETUP = {:postgresql=>proc do |db|
        require 'sequel_core/adapters/jdbc/postgresql'
        db.extend(Sequel::JDBC::Postgres::DatabaseMethods)
        JDBC.load_gem('postgres')
        org.postgresql.Driver
      end,
      :mysql=>proc do |db|
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
      :sqlserver=>proc{com.microsoft.sqlserver.jdbc.SQLServerDriver}
    }
    
    def self.load_gem(name)
      begin
        require "jdbc/#{name}"
      rescue LoadError
        # jdbc gem not used, hopefully the user has the .jar in their CLASSPATH
      end
    end

    class Database < Sequel::Database
      set_adapter_scheme :jdbc
      
      # The type of database we are connecting to
      attr_reader :database_type
      
      def initialize(opts)
        super(opts)
        raise(Error, "No connection string specified") unless uri
        if match = /\Ajdbc:([^:]+)/.match(uri) and prok = DATABASE_SETUP[match[1].to_sym]
          prok.call(self)
        end
      end
      
      def connect
        setup_connection(JavaSQL::DriverManager.getConnection(uri))
      end
      
      def dataset(opts = nil)
        JDBC::Dataset.new(self, opts)
      end
      
      def disconnect
        @pool.disconnect {|c| c.close}
      end
      
      def execute(sql)
        log_info(sql)
        @pool.hold do |conn|
          stmt = conn.createStatement
          begin
            yield stmt.executeQuery(sql)
          rescue NativeException, JavaSQL::SQLException => e
            raise Error, e.message
          ensure
            stmt.close
          end
        end
      end
      
      def execute_ddl(sql)
        log_info(sql)
        @pool.hold do |conn|
          stmt = conn.createStatement
          begin
            stmt.execute(sql)
          rescue NativeException, JavaSQL::SQLException => e
            raise Error, e.message
          ensure
            stmt.close
          end
        end
      end
      
      def execute_dui(sql)
        log_info(sql)
        @pool.hold do |conn|
          stmt = conn.createStatement
          begin
            stmt.executeUpdate(sql)
          rescue NativeException, JavaSQL::SQLException => e
            raise Error, e.message
          ensure
            stmt.close
          end
        end
      end
      
      def setup_connection(conn)
        conn
      end
      
      def uri
        ur = @opts[:uri] || @opts[:url] || @opts[:database]
        ur =~ /^\Ajdbc:/ ? ur : "jdbc:#{ur}"
      end
      alias url uri
      
      private
      
      def connection_pool_default_options
        super.merge(:pool_convert_exceptions=>false)
      end
    end
    
    class Dataset < Sequel::Dataset
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

      def fetch_rows(sql, &block)
        @db.synchronize do
          @db.execute(sql) do |result|
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
    end
  end
end

class Java::JavaSQL::Timestamp
  def usec
    getNanos/1000
  end
end
