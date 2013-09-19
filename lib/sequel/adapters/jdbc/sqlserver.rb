Sequel.require 'adapters/jdbc/mssql'

module Sequel
  module JDBC
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
          def process_result_set_convert(cols, result)
            while result.next
              row = {}
              cols.each do |n, i, p|
                v = (n == :is_autoincrement ? result.getString(i) : result.getObject(i))
                row[n] = if v
                  if p
                    p.call(v)
                  elsif p.nil?
                    cols[i-1][2] = p = convert_type_proc(v)
                    if p
                      p.call(v)
                    else
                      v
                    end
                  else
                    v
                  end
                else
                  v
                end
              end
              yield row
            end
          end

          def process_result_set_no_convert(cols, result)
            while result.next
              row = {}
              cols.each do |n, i|
                row[n] = (n == :is_autoincrement ? result.getString(i) : result.getObject(i))
              end
              yield row
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
