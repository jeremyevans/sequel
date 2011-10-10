Sequel.require 'adapters/shared/oracle'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    # Database and Dataset support for Oracle databases accessed via JDBC.
    module Oracle
      # Instance methods for Oracle Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::Oracle::DatabaseMethods
        include Sequel::JDBC::Transactions
        
        # Return Sequel::JDBC::Oracle::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Oracle::Dataset.new(self, opts)
        end
        
        private

        def schema_parse_table(*)
          sch = super
          sch.each do |c, s|
            if s[:type] == :decimal && s[:scale] == -127
              s[:type] = :integer
            elsif s[:db_type] == 'DATE'
              s[:type] = :datetime
            end
          end
          sch
        end

        def schema_parse_table_skip?(h, schema)
          super || (h[:table_schem] != current_user unless schema)
        end
      end
      
      # Dataset class for Oracle datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Oracle::DatasetMethods
      end
    end
  end
end
