Sequel.require 'adapters/jdbc/mssql'

module Sequel
  module JDBC
    # Database and Dataset instance methods for SQLServer specific
    # support via JDBC.
    module SQLServer
      # Database instance methods for SQLServer databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::JDBC::MSSQL::DatabaseMethods
        
        # Return instance of Sequel::JDBC::SQLServer::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::SQLServer::Dataset.new(self, opts)
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
      
      # Dataset class for SQLServer datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MSSQL::DatasetMethods
      end
    end
  end
end
