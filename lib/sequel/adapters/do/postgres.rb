Sequel.require 'adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << ::DataObjects::Error
  
  module DataObjects
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via DataObjects.
    module Postgres
      # Methods to add to the DataObjects adapter/connection to allow it to work
      # with the shared PostgreSQL code.
      module AdapterMethods
        include Sequel::Postgres::AdapterMethods
        
        # Give the DataObjects adapter a direct execute method, which creates
        # a statement with the given sql and executes it.
        def execute(sql, args=nil)
          command = create_command(sql)
          begin
            if block_given?
              begin
                yield(reader = @db.log_yield(sql){command.execute_reader})
              ensure
                reader.close if reader
              end
            else
              @db.log_yield(sql){command.execute_non_query}
            end
          rescue ::DataObjects::Error => e
            raise_error(e)
          end
        end
        
        private
        
        # DataObjects specific method of getting specific values from a result set.
        def single_value(reader)
          while(reader.next!) do
            return reader.values.at(0)
          end
        end
      end
    
      # Methods to add to Database instances that access PostgreSQL via
      # DataObjects.
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
        
        # Return instance of Sequel::DataObjects::Postgres::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::DataObjects::Postgres::Dataset.new(self, opts)
        end
        
        # Run the INSERT sql on the database and return the primary key
        # for the record.
        def execute_insert(sql, opts={})
          synchronize(opts[:server]) do |conn|
            com = conn.create_command(sql)
            log_yield(sql){com.execute_non_query}
            insert_result(conn, opts[:table], opts[:values])
          end
        end
        
        private
        
        # Extend the adapter with the DataObjects PostgreSQL AdapterMethods
        def setup_connection(conn)
          conn = super(conn)
          conn.extend(Sequel::DataObjects::Postgres::AdapterMethods)
          conn.db = self
          conn.apply_connection_settings
          conn
        end
      end
      
      # Dataset subclass used for datasets that connect to PostgreSQL via DataObjects.
      class Dataset < DataObjects::Dataset
        include Sequel::Postgres::DatasetMethods
      end
    end
  end
end
