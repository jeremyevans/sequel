Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
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
        def last_insert_id(conn, opts={})
          nil
        end

        # AS400 supports transaction isolation levels
        def supports_transaction_isolation_levels?
          true
        end

        private

        # Use JDBC connection's setAutoCommit to false to start transactions
        def begin_transaction(conn, opts={})
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
