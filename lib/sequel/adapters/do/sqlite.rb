Sequel.require 'adapters/shared/sqlite'

module Sequel
  module DataObjects
    # Database and Dataset support for SQLite databases accessed via DataObjects.
    module SQLite
      # Instance methods for SQLite Database objects accessed via DataObjects.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods

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
    end
  end
end
