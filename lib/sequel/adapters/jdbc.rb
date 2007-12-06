require 'java'

module Sequel
  module JDBC
    module JavaLang; include_package 'java.lang'; end
    module JavaSQL; include_package 'java.sql'; end
    
    def self.load_driver(driver)
      JavaLang::Class.forName(driver)
      # "com.mysql.jdbc.Driver"
    end
    
    class Database < Sequel::Database
      set_adapter_scheme :jdbc
      
      def connect
        JavaSQL::DriverManager.getConnection(@opts[:uri], @opts[:user], @opts[:password]);
        # "jdbc:mysql://127.0.0.1:3306/ruby?user=root"
      end
      
      def disconnect
        @pool.disconnect {|c| c.close}
      end
    
      def dataset(opts = nil)
        JDBC::Dataset.new(self, opts)
      end
      
      def execute_and_forget(sql)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          stmt = conn.createStatement
          begin
            stmt.executeQuery(sql)
          ensure
            stmt.close
          end
        end
      end
      
      def execute(sql)
        @logger.info(sql) if @logger
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
        when Time: literal(v.iso8601)
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
            column_count.times {|i| @columns << meta.getColumnName(i).to_sym}

            # get rows
            while result.next
              row = {}
              @columns.each_with_index {|v, i| row[v] = result.getObject(i)}
              yield row
            end
          end
        end
        self
      end
      
      def insert(*values)
        @db.execute_and_forget insert_sql(*values)
      end
    
      def update(values, opts = nil)
        @db.execute_and_forget update_sql(values, opts)
      end
    
      def delete(opts = nil)
        @db.execute_and_forget delete_sql(opts)
      end
    end
  end
end