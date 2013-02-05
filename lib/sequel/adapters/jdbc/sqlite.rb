Sequel.require 'adapters/shared/sqlite'

module Sequel
  module JDBC
    # Database and Dataset support for SQLite databases accessed via JDBC.
    module SQLite
      # Instance methods for SQLite Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods
        LAST_INSERT_ROWID = 'SELECT last_insert_rowid()'.freeze
        FOREIGN_KEY_ERROR_RE = /query does not return ResultSet/.freeze
        
        # Swallow pointless exceptions when the foreign key list pragma
        # doesn't return any rows.
        def foreign_key_list(table, opts={})
          super
        rescue Sequel::DatabaseError => e
          raise unless e.message =~ FOREIGN_KEY_ERROR_RE
          []
        end

        # Swallow pointless exceptions when the index list pragma
        # doesn't return any rows.
        def indexes(table, opts={})
          super
        rescue Sequel::DatabaseError => e
          raise unless e.message =~ FOREIGN_KEY_ERROR_RE
          {}
        end

        private
        
        DATABASE_ERROR_REGEXPS = Sequel::SQLite::DatabaseMethods::DATABASE_ERROR_REGEXPS.merge(/Abort due to constraint violation/ => ConstraintViolation).freeze
        def database_error_regexps
          DATABASE_ERROR_REGEXPS
        end

        # Use last_insert_rowid() to get the last inserted id.
        def last_insert_id(conn, opts={})
          statement(conn) do |stmt|
            rs = stmt.executeQuery(LAST_INSERT_ROWID)
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
    end
  end
end
