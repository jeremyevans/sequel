Sequel.require 'adapters/shared/sqlite'

module Sequel
  module DataObjects
    # Database and Dataset support for SQLite databases accessed via DataObjects.
    module SQLite
      # Instance methods for SQLite Database objects accessed via DataObjects.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods
        
        # Return Sequel::DataObjects::SQLite::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::DataObjects::SQLite::Dataset.new(self, opts)
        end
        
        private
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'sqlite3::memory:' ? o.merge(:max_connections=>1) : o
        end
        
        # Execute the connection pragmas on the connection
        def setup_connection(conn)
          connection_pragmas.each do |s|
            com = conn.create_command(s)
            log_yield(s){com.execute_non_query}
          end
          super
        end
      end
      
      # Dataset class for SQLite datasets accessed via DataObjects.
      class Dataset < DataObjects::Dataset
        include Sequel::SQLite::DatasetMethods
      end
    end
  end
end
