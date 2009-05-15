# Module containing overrides for Sequel's internal transaction method so that
# it works with connection objects that use blocks for transactions (SQLite
# and Amalgalite currently).
module Sequel
  class Database
    module BlockTransactions
      private

      # Use the connection's transaction method with a block to implement transactions.
      def _transaction(conn)
        begin
          result = nil
          log_info(TRANSACTION_BEGIN)
          conn.transaction{result = yield(conn)}
          result
        rescue ::Exception => e
          log_info(TRANSACTION_ROLLBACK)
          transaction_error(e)
        ensure
          log_info(TRANSACTION_COMMIT) unless e
        end
      end
    end
  end
end
