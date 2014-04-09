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
        @supports_savepoints = synchronize{|c| c.getMetaData.supports_savepoints}
      end

      # Check the JDBC DatabaseMetaData for support for serializable isolation,
      # since that's the value most people will use.
      def supports_transaction_isolation_levels?
        synchronize{|conn| conn.getMetaData.supportsTransactionIsolationLevel(JavaSQL::Connection::TRANSACTION_SERIALIZABLE)}
      end

      private

      JDBC_TRANSACTION_ISOLATION_LEVELS = {:uncommitted=>JavaSQL::Connection::TRANSACTION_READ_UNCOMMITTED,
        :committed=>JavaSQL::Connection::TRANSACTION_READ_COMMITTED,
        :repeatable=>JavaSQL::Connection::TRANSACTION_REPEATABLE_READ,
        :serializable=>JavaSQL::Connection::TRANSACTION_SERIALIZABLE}

      # Set the transaction isolation level on the given connection using
      # the JDBC API.
      def set_transaction_isolation(conn, opts)
        level = opts.fetch(:isolation, transaction_isolation_level)
        if (jdbc_level = JDBC_TRANSACTION_ISOLATION_LEVELS[level]) &&
            conn.getMetaData.supportsTransactionIsolationLevel(jdbc_level)
          _trans(conn)[:original_jdbc_isolation_level] = conn.getTransactionIsolation
          log_yield("Transaction.isolation_level = #{level}"){conn.setTransactionIsolation(jdbc_level)}
        end
      end

      # Most JDBC drivers that support savepoints support releasing them.
      def supports_releasing_savepoints?
        true
      end

      # Use JDBC connection's setAutoCommit to false to start transactions
      def begin_transaction(conn, opts=OPTS)
        if supports_savepoints?
          th = _trans(conn)
          if sps = th[:savepoint_objs]
            sps << log_yield(TRANSACTION_SAVEPOINT){conn.set_savepoint}
          else
            log_yield(TRANSACTION_BEGIN){conn.setAutoCommit(false)}
            th[:savepoint_objs] = []
            set_transaction_isolation(conn, opts)
          end
        else
          log_yield(TRANSACTION_BEGIN){conn.setAutoCommit(false)}
          set_transaction_isolation(conn, opts)
        end
      end
      
      # Use JDBC connection's commit method to commit transactions
      def commit_transaction(conn, opts=OPTS)
        if supports_savepoints?
          sps = _trans(conn)[:savepoint_objs]
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
        if jdbc_level = _trans(conn)[:original_jdbc_isolation_level]
          conn.setTransactionIsolation(jdbc_level)
        end
        if supports_savepoints?
          sps = _trans(conn)[:savepoint_objs]
          conn.setAutoCommit(true) if sps.empty?
          sps.pop
        else
          conn.setAutoCommit(true)
        end
      ensure
        super
      end
      
      # Use JDBC connection's rollback method to rollback transactions
      def rollback_transaction(conn, opts=OPTS)
        if supports_savepoints?
          sps = _trans(conn)[:savepoint_objs]
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

