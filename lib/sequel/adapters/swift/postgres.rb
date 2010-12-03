Sequel.require 'adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << ::SwiftError
  
  module Swift
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via Swift.
    module Postgres
      # Methods to add to the Swift adapter/connection to allow it to work
      # with the shared PostgreSQL code.
      module AdapterMethods
        include Sequel::Postgres::AdapterMethods
        
        # Log all SQL that goes through the execute method to the related
        # database object.
        def execute(sql, *args)
          @db.log_yield(sql){super}
        rescue SwiftError => e
          @db.send(:raise_error, e)
        end
        
        private
        
        # Swift specific method of getting specific values from a result set.
        def single_value(row)
          row.values.at(0)
        end
      end
    
      # Methods to add to Database instances that access PostgreSQL via Swift.
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
        
        # Return instance of Sequel::Swift::Postgres::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::Swift::Postgres::Dataset.new(self, opts)
        end
        
        # Run the SELECT SQL on the database and yield the rows
        def execute(sql, opts={})
          synchronize(opts[:server]) do |conn|
            begin
              conn.execute(sql)
              res = conn.results
              yield res if block_given?
            rescue SwiftError => e
              raise_error(e)
            ensure
              res.finish if res
            end
          end
        end
        
        # Run the DELETE/UPDATE SQL on the database and return the number
        # of matched rows.
        def execute_dui(sql, opts={})
          synchronize(opts[:server]) do |conn|
            begin
              conn.execute(sql)
            rescue SwiftError => e
              raise_error(e)
            end
          end
        end
      
        # Run the INSERT SQL on the database and return the primary key
        # for the record.
        def execute_insert(sql, opts={})
          synchronize(opts[:server]) do |conn|
            begin
              conn.execute(sql)
              insert_result(conn, opts[:table], opts[:values])
            rescue SwiftError => e
              raise_error(e)
            end
          end
        end
        
        private
        
        # Execute SQL on the connection.
        def log_connection_execute(conn, sql)
          conn.execute(sql)
        end
      
        # Extend the adapter with the Swift PostgreSQL AdapterMethods.
        def setup_connection(conn)
          conn = super(conn)
          conn.extend(Sequel::Swift::Postgres::AdapterMethods)
          conn.db = self
          conn.apply_connection_settings
          conn
        end
      end
      
      class Dataset < Swift::Dataset
        include Sequel::Postgres::DatasetMethods
      end
    end
  end
end
