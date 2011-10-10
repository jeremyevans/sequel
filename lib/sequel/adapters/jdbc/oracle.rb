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

        def self.extended(db)
          db.instance_eval do
            @autosequence = opts[:autosequence]
            @primary_key_sequences = {}
          end
        end
        
        # Return Sequel::JDBC::Oracle::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Oracle::Dataset.new(self, opts)
        end
        
        private

        def last_insert_id(conn, opts)
          unless sequence = opts[:sequence]
            if t = opts[:table]
              sequence = sequence_for_table(t)
            end
          end
          if sequence
            sql = "SELECT #{literal(sequence)}.currval FROM dual"
            statement(conn) do |stmt|
              begin
                rs = log_yield(sql){stmt.executeQuery(sql)}
                rs.next
                rs.getInt(1)
              rescue java.sql.SQLException
                nil
              end
            end
          end
        end

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

        private

        def convert_type(v)
          case v
          when Java::JavaMath::BigDecimal
            v.scale == 0 ? v.int_value : super
          when Java::OracleSql::TIMESTAMP
            db.to_application_timestamp(v.to_string)
          else
            super
          end
        end
      end
    end
  end
end
