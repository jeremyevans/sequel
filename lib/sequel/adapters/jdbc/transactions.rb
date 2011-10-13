module Sequel
  module JDBC
    module Transactions
      TRANSACTION_BEGIN = 'Transaction.begin'.freeze
      TRANSACTION_COMMIT = 'Transaction.commit'.freeze
      TRANSACTION_ROLLBACK = 'Transaction.rollback'.freeze

      private

      # Use JDBC connection's setAutoCommit to false to start transactions
      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN){conn.setAutoCommit(false)}
      end
      
      # Use JDBC connection's commit method to commit transactions
      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){conn.commit}
      end
      
      # Use JDBC connection's setAutoCommit to true to enable non-transactional behavior
      def remove_transaction(conn, committed)
        conn.setAutoCommit(true)
      ensure
        super
      end
      
      # Use JDBC connection's rollback method to rollback transactions
      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){conn.rollback}
      end
    end
  end
end

