require 'sequel_core/adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << NativeException
  
  module JDBC
    module Postgres
      module AdapterMethods
        include Sequel::Postgres::AdapterMethods
        
        def execute(sql, method=:execute)
          method = :executeQuery if block_given?
          stmt = createStatement
          begin
            rows = stmt.send(method, sql)
            yield(rows) if block_given?
          rescue NativeException => e
            raise Error, e.message
          ensure
            stmt.close
          end
        end
        
        def result_set_values(r, *vals)
          return if r.nil?
          r.next
          return if r.getRow == 0
          case vals.length
          when 1
            r.getString(vals.first+1)
          else
            vals.collect{|col| r.getString(col+1)}
          end
        end
      end
    
      module DatabaseMethods
        include Sequel::Postgres::DatabaseMethods
        
        def dataset(opts=nil)
          Sequel::JDBC::Postgres::Dataset.new(self, opts)
        end
        
        def setup_connection(conn)
          conn.extend(Sequel::JDBC::Postgres::AdapterMethods)
          conn
        end
      end
      
      class Dataset < JDBC::Dataset
        include Sequel::Postgres::DatasetMethods
        
        def literal(v)
          case v
          when SQL::Blob
            "'#{v.gsub(/[\000-\037\047\134\177-\377]/){|b| "\\#{ b[0].to_s(8).rjust(3, '0') }"}}'"
          when Java::JavaSql::Timestamp
            "TIMESTAMP #{literal(v.to_s)}"
          else
            super
          end
        end
      end
    end
  end
end
