require 'sequel_core/adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << NativeException
  
  module JDBC
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via JDBC.
    module Postgres
      # Methods to add to the JDBC adapter/connection to allow it to work
      # with the shared PostgreSQL code.
      module AdapterMethods
        include Sequel::Postgres::AdapterMethods
        
        # Give the JDBC adapter a direct execute method, which creates
        # a statement with the given sql and executes it.
        def execute(sql, args=nil)
          method = block_given? ? :executeQuery : :execute
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
        
        private
        
        # JDBC specific method of getting specific values from a result set.
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
    
      # Methods to add to Database instances that access PostgreSQL via
      # JDBC.
      module DatabaseMethods
        include Sequel::Postgres::DatabaseMethods
        
        # Return instance of Sequel::JDBC::Postgres::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Postgres::Dataset.new(self, opts)
        end
        
        # Run the INSERT sql on the database and return the primary key
        # for the record.
        def execute_insert(sql, opts={})
          super(sql, {:type=>:insert}.merge(opts))
        end
        
        private
        
        # Extend the adapter with the JDBC PostgreSQL AdapterMethods
        def setup_connection(conn)
          conn = super(conn)
          conn.extend(Sequel::JDBC::Postgres::AdapterMethods)
          conn
        end
        
        # Call insert_result with the table and values specified in the opts.
        def last_insert_id(conn, opts)
          insert_result(conn, opts[:table], opts[:values])
        end
      end
      
      # Dataset subclass used for datasets that connect to PostgreSQL via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Postgres::DatasetMethods
        
        # Convert Java::JavaSql::Timestamps correctly, and handle SQL::Blobs
        # correctly.
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
