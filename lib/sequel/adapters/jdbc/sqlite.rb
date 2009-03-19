Sequel.require 'adapters/shared/sqlite'

module Sequel
  module JDBC
    # Database and Dataset support for SQLite databases accessed via JDBC.
    module SQLite
      # Instance methods for SQLite Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods
        
        # Return Sequel::JDBC::SQLite::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::SQLite::Dataset.new(self, opts)
        end
        
        private
        
        # Use last_insert_rowid() to get the last inserted id.
        def last_insert_id(conn, opts={})
          stmt = conn.createStatement
          begin
            rs = stmt.executeQuery('SELECT last_insert_rowid()')
            rs.next
            rs.getInt(1)
          ensure
            stmt.close
          end
        end
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'jdbc:sqlite::memory:' ? o.merge(:max_connections=>1) : o
        end
      end
      
      # Dataset class for SQLite datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::SQLite::DatasetMethods
      end
    end
  end
end
