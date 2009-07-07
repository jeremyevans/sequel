Sequel.require 'adapters/shared/mssql'

module Sequel
  module ODBC
    # Database and Dataset instance methods for MSSQL specific
    # support via ODBC.
    module MSSQL
      module DatabaseMethods
        include Sequel::MSSQL::DatabaseMethods
        LAST_INSERT_ID_SQL='SELECT SCOPE_IDENTITY()'
        
        # Return an instance of Sequel::ODBC::MSSQL::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::ODBC::MSSQL::Dataset.new(self, opts)
        end
        
        # Return the last inserted identity value.
        def execute_insert(sql, opts={})
          log_info(sql)
          synchronize(opts[:server]) do |conn|
            begin
              conn.do(sql)
              log_info(LAST_INSERT_ID_SQL)
              begin
                s = conn.run(LAST_INSERT_ID_SQL)
                if (rows = s.fetch_all) and (row = rows.first)
                  Integer(row.first)
                end
              ensure
                s.drop if s
              end
            rescue ::ODBC::Error => e
              raise_error(e)
            end
          end
        end
      end
      
      class Dataset < ODBC::Dataset
        include Sequel::MSSQL::DatasetMethods
      end
    end
  end
end
