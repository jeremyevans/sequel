if !Object.const_defined?('Sequel')
  require File.join(File.dirname(__FILE__), '../sequel')
end

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
        when Time: literal(v.iso8601)
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
              r.each_with_index {|v, i| row[columns[i]] = v}
              yield row
            end
          ensure
            cursor.close
          end
        end
        self
      end
      
      def hash_row(row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
        end
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
    
      def update(values, opts = nil)
        @db.do update_sql(values, opts)
      end
    
      def delete(opts = nil)
        @db.do delete_sql(opts)
      end

      # Performs the same thing as the stock #insert_sql, but without the 
      # trailing semicolon.
      def insert_sql(*values)
        if values.empty?
          "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
        else
          values = values[0] if values.size == 1
          case values
          when Sequel::Model
            insert_sql(values.values)
          when Array
            if values.empty?
              "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
            elsif values.keys
              fl = values.keys
              vl = transform_save(values.values).map {|v| literal(v)}
              "INSERT INTO #{@opts[:from]} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            else
              "INSERT INTO #{@opts[:from]} VALUES (#{literal(values)})"
            end
          when Hash
            values = transform_save(values) if @transform
            if values.empty?
              "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
            else
              fl, vl = [], []
              values.each {|k, v| fl << column_name(k); vl << literal(v)}
              "INSERT INTO #{@opts[:from]} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            end
          when Dataset
            "INSERT INTO #{@opts[:from]} #{literal(values)}"
          else
            "INSERT INTO #{@opts[:from]} VALUES (#{literal(values)})"
          end
        end
      end
    end
  end
end