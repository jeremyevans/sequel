Sequel.require 'adapters/shared/sqlite'

module Sequel
  module JDBC
    # Database and Dataset support for SQLite databases accessed via JDBC.
    module SQLite
      # Instance methods for SQLite Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods
        
        private
        
        # Use last_insert_rowid() to get the last inserted id.
        def last_insert_id(conn, opts={})
          statement(conn) do |stmt|
            rs = stmt.executeQuery('SELECT last_insert_rowid()')
            rs.next
            rs.getInt(1)
          end
        end
        
        # Default to a single connection for a memory database.
        def connection_pool_default_options
          o = super
          uri == 'jdbc:sqlite::memory:' ? o.merge(:max_connections=>1) : o
        end
        
        # Execute the connection pragmas on the connection.
        def setup_connection(conn)
          conn = super(conn)
          statement(conn) do |stmt|
            connection_pragmas.each{|s| log_yield(s){stmt.execute(s)}}
          end
          conn
        end
      end
      
      # Dataset class for SQLite datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::SQLite::DatasetMethods
      end
    end
  end
end
