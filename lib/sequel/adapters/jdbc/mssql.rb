# frozen-string-literal: true

Sequel.require 'adapters/shared/mssql'

module Sequel
  module JDBC
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
        def last_insert_id(conn, opts=OPTS)
          statement(conn) do |stmt|
            sql = opts[:prepared] ? ATAT_IDENTITY : SCOPE_IDENTITY
            rs = log_yield(sql){stmt.executeQuery(sql)}
            rs.next
            rs.getLong(1)
          end
        end
        
        # Primary key indexes appear to start with pk__ on MSSQL
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
        end
      end
    end
  end
end
