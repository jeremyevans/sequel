Sequel.require 'adapters/jdbc/mssql'

module Sequel
  module JDBC
    # Database and Dataset instance methods for JTDS specific
    # support via JDBC.
    module JTDS
      module DatabaseMethods
        include Sequel::JDBC::MSSQL::DatabaseMethods

        private

        # Handle nil values by using setNull with the correct parameter type.
        def set_ps_arg_nil(cps, i)
          cps.setNull(i, cps.getParameterMetaData.getParameterType(i))
        end
      end

      # Dataset class for JTDS datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MSSQL::DatasetMethods

        class ::Sequel::JDBC::Dataset::TYPE_TRANSLATOR
          def jtds_clob(v) Sequel::SQL::Blob.new(v.getSubString(1, v.length)) end
        end

        JTDS_CLOB_METHOD = TYPE_TRANSLATOR_INSTANCE.method(:jtds_clob)
      
        # Handle CLOB types retrieved via JTDS.
        def convert_type_proc(v)
          if v.is_a?(Java::NetSourceforgeJtdsJdbc::ClobImpl)
            JTDS_CLOB_METHOD
          else
            super
          end
        end
      end
    end
  end
end
