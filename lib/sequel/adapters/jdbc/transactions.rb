module Sequel
  module JDBC
    module Transactions
      TRANSACTION_BEGIN = 'Transaction.begin'.freeze
      TRANSACTION_COMMIT = 'Transaction.commit'.freeze
      TRANSACTION_RELEASE_SP = 'Transaction.release_savepoint'.freeze
      TRANSACTION_ROLLBACK = 'Transaction.rollback'.freeze
      TRANSACTION_ROLLBACK_SP = 'Transaction.rollback_savepoint'.freeze
      TRANSACTION_SAVEPOINT= 'Transaction.savepoint'.freeze

      # Check the JDBC DatabaseMetaData for savepoint support
      def supports_savepoints?
        return @supports_savepoints if defined?(@supports_savepoints)
        @supports_savepoints = synchronize{|c| c.get_meta_data.supports_savepoints}
      end

      private

      # Most JDBC drivers that support savepoints support releasing them.
      def supports_releasing_savepoints?
        true
      end

      # Use JDBC connection's setAutoCommit to false to start transactions
      def begin_transaction(conn, opts={})
        if supports_savepoints?
          th = _trans(conn)
          if sps = th[:savepoints]
            sps << log_yield(TRANSACTION_SAVEPOINT){conn.set_savepoint}
          else
            log_yield(TRANSACTION_BEGIN){conn.setAutoCommit(false)}
            th[:savepoints] = []
          end
          th[:savepoint_level] += 1
        else
          log_yield(TRANSACTION_BEGIN){conn.setAutoCommit(false)}
        end
      end

      # Use JDBC connection's commit method to commit transactions
      def commit_transaction(conn, opts={})
        if supports_savepoints?
          sps = _trans(conn)[:savepoints]
          if sps.empty?
            log_yield(TRANSACTION_COMMIT){conn.commit}
          elsif supports_releasing_savepoints?
            log_yield(TRANSACTION_RELEASE_SP){supports_releasing_savepoints? ? conn.release_savepoint(sps.last) : sps.last}
          end
        else
          log_yield(TRANSACTION_COMMIT){conn.commit}
        end
      end

      # Use JDBC connection's setAutoCommit to true to enable non-transactional behavior
      def remove_transaction(conn, committed)
        if supports_savepoints?
          sps = _trans(conn)[:savepoints]
          conn.setAutoCommit(true) if sps.empty?
          sps.pop
        else
          conn.setAutoCommit(true)
        end
      ensure
        super
      end

      # Use JDBC connection's rollback method to rollback transactions
      def rollback_transaction(conn, opts={})
        if supports_savepoints?
          sps = _trans(conn)[:savepoints]
          if sps.empty?
            log_yield(TRANSACTION_ROLLBACK){conn.rollback}
          else
            log_yield(TRANSACTION_ROLLBACK_SP){conn.rollback(sps.last)}
          end
        else
          log_yield(TRANSACTION_ROLLBACK){conn.rollback}
        end
      end
    end
  end
end

