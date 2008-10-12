require 'sequel_core/adapters/shared/oracle'

module Sequel
  module JDBC
    # Database and Dataset support for Oracle databases accessed via JDBC.
    module Oracle
      # Instance methods for Oracle Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::Oracle::DatabaseMethods
        
        # Return Sequel::JDBC::Oracle::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Oracle::Dataset.new(self, opts)
        end
      end
      
      # Dataset class for Oracle datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Oracle::DatasetMethods
      end
    end
  end
end
