Sequel.require 'adapters/jdbc/mssql'

module Sequel
  module JDBC
    # Database and Dataset instance methods for JTDS specific
    # support via JDBC.
    module JTDS
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::JDBC::MSSQL::DatabaseMethods

        private

        # JTDS exception handling with SQLState is less accurate than with regexps.
        def database_exception_use_sqlstates?
          false
        end

        # Handle nil values by using setNull with the correct parameter type.
        def set_ps_arg_nil(cps, i)
          cps.setNull(i, cps.getParameterMetaData.getParameterType(i))
        end
      end

      # Dataset class for JTDS datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MSSQL::DatasetMethods
      end
    end
  end
end
