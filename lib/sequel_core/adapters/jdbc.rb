require 'java'

module Sequel
  module JDBC
    module JavaLang; include_package 'java.lang'; end
    module JavaSQL; include_package 'java.sql'; end
    CLASS_NAMES = {'postgresql'=>'org.postgresql.Driver',
      'mysql'=>'com.mysql.jdbc.Driver',
      'sqlite'=>'org.sqlite.JDBC',
      'oracle'=>'oracle.jdbc.driver.OracleDriver',
      'sqlserver'=>'com.microsoft.sqlserver.jdbc.SQLServerDriver'}
    
    def self.load_driver(driver)
      JavaLang::Class.forName(driver)
    end

    class Database < Sequel::Database
      set_adapter_scheme :jdbc
      
      def connect
        raise(Error, "No connection string specified") unless conn_string = @opts[:uri] || @opts[:url] || @opts[:database]
        conn_string = "jdbc:#{conn_string}" unless conn_string =~ /^\Ajdbc:/
        if match = /\Ajdbc:([^:]+)/.match(conn_string) and jdbc_class_name = CLASS_NAMES[match[1]]
          Sequel::JDBC.load_driver(jdbc_class_name)
        end
        JavaSQL::DriverManager.getConnection(conn_string)
      end
      
      def disconnect
        @pool.disconnect {|c| c.close}
      end
    
      def dataset(opts = nil)
        JDBC::Dataset.new(self, opts)
      end
      
      def execute_and_forget(sql)
        log_info(sql)
        @pool.hold do |conn|
          stmt = conn.createStatement
          begin
            stmt.executeUpdate(sql)
          ensure
            stmt.close
          end
        end
      end
      
      def execute(sql)
        log_info(sql)
        @pool.hold do |conn|
          stmt = conn.createStatement
          begin
            yield stmt.executeQuery(sql)
          ensure
            stmt.close
          end
        end
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
      
      def insert(*values)
        @db.execute_and_forget insert_sql(*values)
      end
    
      def update(*args, &block)
        @db.execute_and_forget update_sql(*args, &block)
      end
    
      def delete(opts = nil)
        @db.execute_and_forget delete_sql(opts)
      end
    end
  end
end
