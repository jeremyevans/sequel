Sequel.require 'adapters/shared/mssql'

module Sequel
  module ADO
    # Database and Dataset instance methods for MSSQL specific
    # support via ADO.
    module MSSQL
      module DatabaseMethods
        include Sequel::MSSQL::DatabaseMethods
        
        # Return instance of Sequel::ADO::MSSQL::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::ADO::MSSQL::Dataset.new(self, opts)
        end
      end
      
      class Dataset < ADO::Dataset
        include Sequel::MSSQL::DatasetMethods
        
        # Use a nasty hack of multiple SQL statements in the same call and
        # having the last one return the most recently inserted id.  This
        # is necessary as ADO doesn't provide a consistent native connection.
        def insert(values={})
          return super if @opts[:sql]
          with_sql("SET NOCOUNT ON; #{insert_sql(values)}; SELECT SCOPE_IDENTITY()").single_value
        end
      end
    end
  end
end
