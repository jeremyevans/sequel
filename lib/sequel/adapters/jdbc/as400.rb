# frozen-string-literal: true

Sequel::JDBC.load_driver('com.ibm.as400.access.AS400JDBCDriver')
Sequel.require 'adapters/jdbc/transactions'
Sequel.require 'adapters/utils/emulate_offset_with_row_number'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:as400] = proc do |db|
        db.extend(Sequel::JDBC::AS400::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::AS400::Dataset
        com.ibm.as400.access.AS400JDBCDriver
      end
    end

    # Database and Dataset support for AS400 databases accessed via JDBC.
    module AS400
      # Instance methods for AS400 Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::JDBC::Transactions

        TRANSACTION_BEGIN = 'Transaction.begin'.freeze
        TRANSACTION_COMMIT = 'Transaction.commit'.freeze
        TRANSACTION_ROLLBACK = 'Transaction.rollback'.freeze
        
        # AS400 uses the :as400 database type.
        def database_type
          :as400
        end

        # TODO: Fix for AS400
        def last_insert_id(conn, opts=OPTS)
          nil
        end

        # AS400 supports transaction isolation levels
        def supports_transaction_isolation_levels?
          true
        end

        private

        def disconnect_error?(exception, opts)
          super || exception.message =~ /\AThe connection does not exist\./
        end

        # Use JDBC connection's setAutoCommit to false to start transactions
        def begin_transaction(conn, opts=OPTS)
          set_transaction_isolation(conn, opts)
          super
        end
      end
      
      # Dataset class for AS400 datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include EmulateOffsetWithRowNumber

        WILDCARD = Sequel::LiteralString.new('*').freeze
        FETCH_FIRST_ROW_ONLY = " FETCH FIRST ROW ONLY".freeze
        FETCH_FIRST = " FETCH FIRST ".freeze
        ROWS_ONLY = " ROWS ONLY".freeze
        
        # Modify the sql to limit the number of rows returned
        def select_limit_sql(sql)
          if l = @opts[:limit]
            if l == 1
              sql << FETCH_FIRST_ROW_ONLY
            elsif l > 1
              sql << FETCH_FIRST
              literal_append(sql, l)
              sql << ROWS_ONLY
            end
          end
        end
          
        def supports_window_functions?
          true
        end
      end
    end
  end
end
