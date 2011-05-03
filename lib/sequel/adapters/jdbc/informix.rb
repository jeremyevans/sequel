Sequel.require 'adapters/shared/informix'

module Sequel
  module JDBC
    # Database and Dataset instance methods for Informix specific
    # support via JDBC.
    module Informix
      # Database instance methods for Informix databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::Informix::DatabaseMethods
        
        # Return instance of Sequel::JDBC::Informix::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Informix::Dataset.new(self, opts)
        end
        
        private
        
        # TODO: implement
        def last_insert_id(conn, opts={})
          nil
        end
      end
      
      # Dataset class for Informix datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Informix::DatasetMethods
      end
    end
  end
end
