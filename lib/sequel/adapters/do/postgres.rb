Sequel.require 'adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << ::DataObjects::Error
  
  module DataObjects
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via DataObjects.
    module Postgres
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
        
        private
        
        # Extend the adapter with the DataObjects PostgreSQL AdapterMethods
        def setup_connection(conn)
          conn = super(conn)
          connection_configuration_sqls.each{|sql| log_yield(sql){conn.create_command(sql).execute_non_query}}
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
