# Module included in adapters that support savepoints, currently MySQL and PostgreSQL.
module Sequel
  class Database
    module SavepointTransactions
      SQL_SAVEPOINT = 'SAVEPOINT autopoint_%d'.freeze
      SQL_ROLLBACK_TO_SAVEPOINT = 'ROLLBACK TO SAVEPOINT autopoint_%d'.freeze
      SQL_RELEASE_SAVEPOINT = 'RELEASE SAVEPOINT autopoint_%d'.freeze
      
      # Any adapter that includes this module must support savepoints.
      def supports_savepoints?
        true
      end
      
      private
      
      # Don't add the current thread to the list of threads with active
      # connections if it is already in the list.  If there isn't an
      # active transaction on this thread, set the transaction depth to
      # zero.
      def add_transaction
        th = Thread.current
        unless @transactions.include?(th)
          th[:sequel_transaction_depth] = 0
          super
        end
      end
      
      # If a savepoint is requested, don't reuse an existing transaction
      def already_in_transaction?(conn, opts)
        super and !opts[:savepoint]
      end
      
      # SQL to start a new savepoint
      def begin_savepoint_sql(depth)
        SQL_SAVEPOINT % depth
      end
      
      # If there is no active transaction, start a new transaction. Otherwise,
      # start a savepoint inside the current transaction.  Increment the 
      def begin_transaction(conn)
        th = Thread.current
        depth = th[:sequel_transaction_depth]
        conn = transaction_statement_object(conn) if respond_to?(:transaction_statement_object, true)
        log_connection_execute(conn, depth > 0 ? begin_savepoint_sql(depth) : begin_transaction_sql)
        th[:sequel_transaction_depth] += 1
        conn
      end
      
      # SQL to commit a savepoint
      def commit_savepoint_sql(depth)
        SQL_RELEASE_SAVEPOINT % depth
      end
      
      # If currently inside a savepoint, commit/release the savepoint.
      # Otherwise, commit the transaction.
      def commit_transaction(conn)
        depth = Thread.current[:sequel_transaction_depth]
        log_connection_execute(conn, depth > 1 ? commit_savepoint_sql(depth-1) : commit_transaction_sql)
      end
      
      # Decrement the current savepoint/transaction depth
      def remove_transaction(conn)
        depth = (Thread.current[:sequel_transaction_depth] -= 1)
        super unless depth > 0
      end
      
      # SQL to rollback to a savepoint
      def rollback_savepoint_sql(depth)
        SQL_ROLLBACK_TO_SAVEPOINT % depth
      end
      
      # If currently inside a savepoint, rollback to the start of the savepoint.
      # Otherwise, rollback the entire transaction.
      def rollback_transaction(conn)
        depth = Thread.current[:sequel_transaction_depth]
        log_connection_execute(conn, depth > 1 ? rollback_savepoint_sql(depth-1) : rollback_transaction_sql)
      end
    end
  end
end
