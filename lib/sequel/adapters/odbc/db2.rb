Sequel.require 'adapters/shared/db2'

module Sequel
  module ODBC
    # Database and Dataset instance methods for DB2 specific
    # support via ODBC.
    module DB2
      module DatabaseMethods
        include ::Sequel::DB2::DatabaseMethods
      end
      
      class Dataset < ODBC::Dataset
        include ::Sequel::DB2::DatasetMethods
      end
    end
  end
end
