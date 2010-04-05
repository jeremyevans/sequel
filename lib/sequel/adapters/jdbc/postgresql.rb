Sequel.require 'adapters/shared/postgres'

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
            rows = @db.log_yield(sql){stmt.send(method, sql)}
            yield(rows) if block_given?
          rescue NativeException => e
            raise_error(e)
          ensure
            stmt.close
          end
        end
        
        private
        
        # JDBC specific method of getting specific values from a result set.
        def single_value(r)
          unless r.nil?
            r.next
            r.getString(1) unless r.getRow == 0
          end
        end
      end
    
      # Methods to add to Database instances that access PostgreSQL via
      # JDBC.
      module DatabaseMethods
        include Sequel::Postgres::DatabaseMethods
        
        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          db.instance_eval do
            @primary_keys = {}
            @primary_key_sequences = {}
          end
        end
        
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
          conn.db = self
          conn.apply_connection_settings
          conn
        end
        
        # Call insert_result with the table and values specified in the opts.
        def last_insert_id(conn, opts)
          insert_result(conn, opts[:table], opts[:values])
        end
        
        # Override shared postgresql adapter method to actually log,
        # since on JDBC the first argument is a statement and not a
        # connection, so it wouldn't be logged otherwise.
        def log_connection_execute(stmt, sql) 
          log_yield(sql){stmt.execute(sql)}
        end
      end
      
      # Dataset subclass used for datasets that connect to PostgreSQL via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Postgres::DatasetMethods

        # Add the shared PostgreSQL prepared statement methods
        def prepare(*args)
          ps = super
          ps.extend(::Sequel::Postgres::DatasetMethods::PreparedStatementMethods)
          ps
        end
        
        # Literalize strings similar to the native postgres adapter
        def literal_string(v)
          db.synchronize{|c| "'#{c.escape_string(v)}'"}
        end
      end
    end
  end
end
