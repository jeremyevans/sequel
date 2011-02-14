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
        PRIMARY_KEY_INDEX_RE = /\Apk__/i.freeze
        
        include Sequel::MSSQL::DatabaseMethods
        
        # Return instance of Sequel::JDBC::MSSQL::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::MSSQL::Dataset.new(self, opts)
        end
        
        private
        
        # Get the last inserted id using SCOPE_IDENTITY().
        def last_insert_id(conn, opts={})
          statement(conn) do |stmt|
            sql = opts[:prepared] ? 'SELECT @@IDENTITY' : 'SELECT SCOPE_IDENTITY()'
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

        def metadata_dataset
          ds = super
          # Work around a bug in SQL Server JDBC Driver 3.0, where the metadata
          # for the getColumns result set specifies an incorrect type for the
          # IS_AUTOINCREMENT column. The column is a string, but the type is
          # specified as a short. This causes getObject() to throw a
          # com.microsoft.sqlserver.jdbc.SQLServerException: "The conversion
          # from char to SMALLINT is unsupported." Using getString() rather
          # than getObject() for this column avoids the problem.
          # Reference: http://social.msdn.microsoft.com/Forums/en/sqldataaccess/thread/20df12f3-d1bf-4526-9daa-239a83a8e435
          def ds.result_set_object_getter
            lambda do |result, n, i|
              if n == :is_autoincrement
                @convert_types ? convert_type(result.getString(i)) : result.getString(i)
              else
                @convert_types ? convert_type(result.getObject(i)) : result.getObject(i)
              end
            end
          end
          ds
        end
      end
      
      # Dataset class for MSSQL datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MSSQL::DatasetMethods
      end
    end
  end
end
