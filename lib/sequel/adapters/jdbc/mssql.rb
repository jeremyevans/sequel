# frozen-string-literal: true

require_relative '../shared/mssql'

module Sequel
  module JDBC
    module MSSQL
      module DatabaseMethods
        include Sequel::MSSQL::DatabaseMethods
        
        private
        
        # Get the last inserted id using SCOPE_IDENTITY().
        def last_insert_id(conn, opts=OPTS)
          if (stmt = opts[:stmt]) && opts[:prepared]
            rs = stmt.getGeneratedKeys
            begin
              if rs.next
                begin
                  rs.getLong(1)
                rescue
                  rs.getObject(1) rescue nil
                end
              end
            ensure
              rs.close
            end
          else
            statement(conn) do |stmt|
              sql = 'SELECT SCOPE_IDENTITY()'
              rs = log_connection_yield(sql, conn){stmt.executeQuery(sql)}
              rs.next
              rs.getLong(1)
            end
          end
        end

        def prepare_jdbc_statement(conn, sql, opts)
          if opts[:type] == :insert
            conn.prepareStatement(sql, JavaSQL::Statement::RETURN_GENERATED_KEYS)
          else
            super
          end
        end
        
        # Primary key indexes appear to start with pk__ on MSSQL
        def primary_key_index_re
          /\Apk__/i
        end
      end
    end
  end
end
