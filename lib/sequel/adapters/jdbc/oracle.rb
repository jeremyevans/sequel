Sequel.require 'adapters/shared/oracle'

module Sequel
  module JDBC
    # Database and Dataset support for Oracle databases accessed via JDBC.
    module Oracle
      # Instance methods for Oracle Database objects accessed via JDBC.
      module DatabaseMethods
        include Sequel::Oracle::DatabaseMethods
        TRANSACTION_BEGIN = 'Transaction.begin'.freeze
        TRANSACTION_COMMIT = 'Transaction.commit'.freeze
        TRANSACTION_ROLLBACK = 'Transaction.rollback'.freeze
        
        # Return Sequel::JDBC::Oracle::Dataset object with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::Oracle::Dataset.new(self, opts)
        end
        
        private
        
        # Use JDBC connection's setAutoCommit to false to start transactions
        def begin_transaction(conn)
          log_yield(TRANSACTION_BEGIN){conn.setAutoCommit(false)}
          conn
        end
        
        # Use JDBC connection's commit method to commit transactions
        def commit_transaction(conn)
          log_yield(TRANSACTION_COMMIT){conn.commit}
        end
        
        # Use JDBC connection's setAutoCommit to true to enable non-transactional behavior
        def remove_transaction(conn)
          conn.setAutoCommit(true) if conn
          super
        end
        
        # Use JDBC connection's rollback method to rollback transactions
        def rollback_transaction(conn)
          log_yield(TRANSACTION_ROLLBACK){conn.rollback}
        end
      end
      
      # Dataset class for Oracle datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Oracle::DatasetMethods
      end
    end
  end
end
