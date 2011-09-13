Sequel.require 'adapters/shared/db2'

module Sequel
  module JDBC
    # Database and Dataset instance methods for DB2 specific
    # support via JDBC.
    module DB2
      # Database instance methods for DB2 databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::DB2::DatabaseMethods
        
        # Return instance of Sequel::JDBC::DB2::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::DB2::Dataset.new(self, opts)
        end
        
        private
        
        # TODO: implement
        def last_insert_id(conn, opts={})
          nil
        end
      end
      
      # Dataset class for DB2 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::DB2::DatasetMethods
      end
    end
  end
end
