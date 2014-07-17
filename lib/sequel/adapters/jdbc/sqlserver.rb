Sequel::JDBC.load_driver('com.microsoft.sqlserver.jdbc.SQLServerDriver')
Sequel.require 'adapters/jdbc/mssql'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:sqlserver] = proc do |db|
        db.extend(Sequel::JDBC::SQLServer::DatabaseMethods)
        db.extend_datasets Sequel::MSSQL::DatasetMethods
        db.send(:set_mssql_unicode_strings)
        com.microsoft.sqlserver.jdbc.SQLServerDriver
      end
    end

    # Database and Dataset instance methods for SQLServer specific
    # support via JDBC.
    module SQLServer
      # Database instance methods for SQLServer databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::JDBC::MSSQL::DatabaseMethods

        # Work around a bug in SQL Server JDBC Driver 3.0, where the metadata
        # for the getColumns result set specifies an incorrect type for the
        # IS_AUTOINCREMENT column. The column is a string, but the type is
        # specified as a short. This causes getObject() to throw a
        # com.microsoft.sqlserver.jdbc.SQLServerException: "The conversion
        # from char to SMALLINT is unsupported." Using getString() rather
        # than getObject() for this column avoids the problem.
        # Reference: http://social.msdn.microsoft.com/Forums/en/sqldataaccess/thread/20df12f3-d1bf-4526-9daa-239a83a8e435
        module MetadataDatasetMethods
          def type_convertor(map, meta, type, i)
            if output_identifier(meta.getColumnLabel(i)) == :is_autoincrement
              map[Java::JavaSQL::Types::VARCHAR]
            else
              super
            end
          end

          def basic_type_convertor(map, meta, type, i)
            if output_identifier(meta.getColumnLabel(i)) == :is_autoincrement
              map[Java::JavaSQL::Types::VARCHAR]
            else
              super
            end
          end
        end
        
        def metadata_dataset
          super.extend(MetadataDatasetMethods)
        end

        private

        def disconnect_error?(exception, opts)
          super || (exception.message =~ /connection is closed/)
        end
      end
    end
  end
end
