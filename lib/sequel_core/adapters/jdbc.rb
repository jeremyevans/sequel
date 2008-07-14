require 'java'

module Sequel
  module JDBC
    module JavaLang; include_package 'java.lang'; end
    module JavaSQL; include_package 'java.sql'; end
    CLASS_NAMES = {:postgresql=>'org.postgresql.Driver',
      :mysql=>'com.mysql.jdbc.Driver',
      :sqlite=>'org.sqlite.JDBC',
      :oracle=>'oracle.jdbc.driver.OracleDriver',
      :sqlserver=>'com.microsoft.sqlserver.jdbc.SQLServerDriver'}
    DATABASE_SETUP = {:postgresql=>proc do |db|
        require 'sequel_core/adapters/jdbc/postgresql'
        db.extend(Sequel::JDBC::Postgres::DatabaseMethods)
      end
    }
    
    def self.load_driver(driver)
      JavaLang::Class.forName(driver)
    end

    class Database < Sequel::Database
      set_adapter_scheme :jdbc
      
      # The type of database we are connecting to
      attr_reader :database_type
      
      def connect
        raise(Error, "No connection string specified") unless conn_string = @opts[:uri] || @opts[:url] || @opts[:database]
        conn_string = "jdbc:#{conn_string}" unless conn_string =~ /^\Ajdbc:/
        if match = /\Ajdbc:([^:]+)/.match(conn_string)
          @database_type = match[1].to_sym
          DATABASE_SETUP[@database_type].call(self)
          if jdbc_class_name = CLASS_NAMES[@database_type]
            Sequel::JDBC.load_driver(jdbc_class_name)
          end
        end
        conn = JavaSQL::DriverManager.getConnection(conn_string)
        setup_connection(conn)
        conn
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
          rescue NativeException => e
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
          rescue NativeException => e
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
          rescue NativeException => e
            raise Error, e.message
          ensure
            stmt.close
          end
        end
      end
      
      def setup_connection(conn)
      end
      
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
        when Date, DateTime
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
