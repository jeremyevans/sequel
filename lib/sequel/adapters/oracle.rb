require 'oci8'

module Sequel
  module Oracle
    class Database < Sequel::Database
      set_adapter_scheme :oracle
      
      # AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      # 
      # def auto_increment_sql
      #   AUTO_INCREMENT
      # end

      def connect
        if @opts[:database]
          dbname = @opts[:host] ? \
            "//#{@opts[:host]}/#{@opts[:database]}" : @opts[:database]
        else
          dbname = @opts[:host]
        end
        conn = OCI8.new(@opts[:user], @opts[:password], dbname, @opts[:privilege])
        conn.autocommit = true
        conn.non_blocking = true
        conn
      end
      
      def disconnect
        @pool.disconnect {|c| c.logoff}
      end
    
      def dataset(opts = nil)
        Oracle::Dataset.new(self, opts)
      end
    
      def execute(sql)
        @logger.info(sql) if @logger
        @pool.hold {|conn| conn.exec(sql)}
      end
      
      alias_method :do, :execute
    end
    
    class Dataset < Sequel::Dataset
      def literal(v)
        case v
        when Time
          literal(v.iso8601)
        else
          super
        end
      end

      def fetch_rows(sql, &block)
        @db.synchronize do
          cursor = @db.execute sql
          begin
            @columns = cursor.get_col_names.map {|c| c.to_sym}
            while r = cursor.fetch
              row = {}
              r.each_with_index {|v, i| row[@columns[i]] = v}
              yield row
            end
          ensure
            cursor.close
          end
        end
        self
      end
      
      def array_tuples_fetch_rows(sql, &block)
        @db.synchronize do
          cursor = @db.execute sql
          begin
            @columns = cursor.get_col_names.map {|c| c.to_sym}
            while r = cursor.fetch
              r.keys = columns
              yield r
            end
          ensure
            cursor.close
          end
        end
        self
      end
      
      def insert(*values)
        @db.do insert_sql(*values)
      end
    
      def update(*args, &block)
        @db.do update_sql(*args, &block)
      end
    
      def delete(opts = nil)
        @db.do delete_sql(opts)
      end
    end
  end
end