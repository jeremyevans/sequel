Sequel.require 'adapters/shared/oracle'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    # Database and Dataset support for Oracle databases accessed via JDBC.
    module Oracle
      # Instance methods for Oracle Database objects accessed via JDBC.
      module DatabaseMethods
        PRIMARY_KEY_INDEX_RE = /\Asys_/i.freeze

        include Sequel::Oracle::DatabaseMethods
        include Sequel::JDBC::Transactions

        def self.extended(db)
          db.instance_eval do
            @autosequence = opts[:autosequence]
            @primary_key_sequences = {}
          end
        end
        
        private

        # Oracle exception handling with SQLState is less accurate than with regexps.
        def database_exception_use_sqlstates?
          false
        end

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

        # Primary key indexes appear to start with sys_ on Oracle
        def primary_key_index_re
          PRIMARY_KEY_INDEX_RE
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

        # As of Oracle 9.2, releasing savepoints is no longer supported.
        def supports_releasing_savepoints?
          false
        end
      end
      
      # Dataset class for Oracle datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Oracle::DatasetMethods

        private

        JAVA_BIG_DECIMAL = ::Sequel::JDBC::Dataset::JAVA_BIG_DECIMAL
        JAVA_BIG_DECIMAL_CONSTRUCTOR = java.math.BigDecimal.java_class.constructor(Java::long).method(:new_instance)

        class ::Sequel::JDBC::Dataset::TYPE_TRANSLATOR
          def oracle_decimal(v)
            if v.scale == 0
              i = v.long_value
              if v.equals(JAVA_BIG_DECIMAL_CONSTRUCTOR.call(i))
                i
              else
                decimal(v)
              end
            else
              decimal(v)
            end
          end
        end

        ORACLE_DECIMAL_METHOD = TYPE_TRANSLATOR_INSTANCE.method(:oracle_decimal)

        def convert_type_oracle_timestamp(v)
          db.to_application_timestamp(v.to_string)
        end
      
        def convert_type_oracle_timestamptz(v)
          convert_type_oracle_timestamp(db.synchronize{|c| v.timestampValue(c)})
        end
      
        def convert_type_proc(v)
          case v
          when JAVA_BIG_DECIMAL
            ORACLE_DECIMAL_METHOD
          when Java::OracleSql::TIMESTAMPTZ
            method(:convert_type_oracle_timestamptz)
          when Java::OracleSql::TIMESTAMP
            method(:convert_type_oracle_timestamp)
          else
            super
          end
        end
      end
    end
  end
end
