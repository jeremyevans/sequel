Sequel.require 'adapters/jdbc/mssql'

module Sequel
  module JDBC
    # Database and Dataset instance methods for JTDS specific
    # support via JDBC.
    module JTDS
      # Database instance methods for JTDS databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::JDBC::MSSQL::DatabaseMethods
      end
      
      # Dataset class for JTDS datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MSSQL::DatasetMethods

        # Handle CLOB types retrieved via JTDS.
        def convert_type(v)
          case v
          when Java::NetSourceforgeJtdsJdbc::ClobImpl
            convert_type(v.getSubString(1, v.length))
          else
            super
          end
        end
      end
    end
  end
end
