# frozen-string-literal: true
#
# The transaction_connection_validator extension automatically
# retries a transaction on a connection if an disconnect error
# is raised when sending the statement to begin a new
# transaction, as long as the user has not already checked out
# a connection.  This is safe to do because no other queries
# have been issued on the connection, and no user-level code
# is run before retrying.
#
# This approach to connection validation can be significantly
# lower overhead than the connection_validator extension,
# though it does not handle all cases handled by the
# connection_validator extension.  However, it performs the
# validation checks on every new transaction, so it will
# automatically handle disconnected connections in some cases
# where the connection_validator extension will not by default
# (as the connection_validator extension only checks
# connections if they have not been used in the last hour by
# default).
#
# Related module: Sequel::TransactionConnectionValidator

#
module Sequel
  module TransactionConnectionValidator
    class DisconnectRetry < DatabaseDisconnectError
      # The connection that raised the disconnect error
      attr_accessor :connection

      # The underlying disconnect error, in case it needs to be reraised.
      attr_accessor :database_error
    end

    # Rescue disconnect errors raised when beginning a new transaction.  If there
    # is a disconnnect error, it should be safe to retry the transaction using a
    # new connection, as we haven't yielded control to the user yet.
    def transaction(opts=OPTS)
      super
    rescue DisconnectRetry => e
      if synchronize(opts[:server]){|conn| conn.equal?(e.connection)}
        # If retrying would use the same connection, that means the
        # connection was not removed from the pool, which means the caller has
        # already checked out the connection, and retrying will not be successful.
        # In this case, we can only reraise the exception.
        raise e.database_error
      end

      num_retries ||= 0 
      num_retries += 1
      retry if num_retries < 5

      raise e.database_error
    end

    private

    # Reraise disconnect errors as DisconnectRetry so they can be retried.
    def begin_new_transaction(conn, opts)
      super
    rescue Sequel::DatabaseDisconnectError, *database_error_classes => e
      if e.is_a?(Sequel::DatabaseDisconnectError) || disconnect_error?(e, OPTS)
        exception = DisconnectRetry.new(e.message)
        exception.set_backtrace([])
        exception.connection = conn
        unless e.is_a?(Sequel::DatabaseError)
          e = Sequel.convert_exception_class(e, database_error_class(e, OPTS))
        end
        exception.database_error = e
        raise exception
      end

      raise
    end
  end

  Database.register_extension(:transaction_connection_validator, TransactionConnectionValidator)
end
