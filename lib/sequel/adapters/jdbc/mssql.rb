Sequel.require 'adapters/shared/mssql'

module Sequel
  module JDBC
    class Database
      # Alias the generic JDBC version so it can be called directly later
      alias jdbc_schema_parse_table schema_parse_table
    end
    
    # Database and Dataset instance methods for MSSQL specific
    # support via JDBC.
    module MSSQL
      # Database instance methods for MSSQL databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        PRIMARY_KEY_INDEX_RE = /\Apk__/i.freeze
        ATAT_IDENTITY = 'SELECT @@IDENTITY'.freeze
        SCOPE_IDENTITY = 'SELECT SCOPE_IDENTITY()'.freeze
        
        include Sequel::MSSQL::DatabaseMethods
        
        private
        
        # Get the last inserted id using SCOPE_IDENTITY().
        def last_insert_id(conn, opts={})
          statement(conn) do |stmt|
            sql = opts[:prepared] ? ATAT_IDENTITY : SCOPE_IDENTITY
            rs = log_yield(sql){stmt.executeQuery(sql)}
            rs.next
            rs.getInt(1)
          end
        end
        
        # Call the generic JDBC version instead of MSSQL version,
        # since the JDBC version handles primary keys.
        def schema_parse_table(table, opts={})
          jdbc_schema_parse_table(table, opts)
        end
        
        # Primary key indexes appear to start with pk__ on MSSQL
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end
    end
  end
end
