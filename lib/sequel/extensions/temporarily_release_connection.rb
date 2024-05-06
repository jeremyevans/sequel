# frozen-string-literal: true
#
# The temporarily_release_connection extension adds support for temporarily
# releasing a checked out connection back to the connection pool.  It is
# designed for use in multithreaded transactional integration tests, allowing
# a connection to start a transaction in one thread, but be temporarily
# released back to the connection pool, so it can be operated on safely
# by multiple threads inside a block. For example, the main thread could be
# running tests that send web requests, and a separate thread running a web
# server that is responding to those requests, and the same connection and
# transaction would be used for both.
#
# To load the extension into the database:
#
#   DB.extension :temporarily_release_connection
#
# After the extension is loaded, call the +temporarily_release_connection+
# method with the connection object to temporarily release the connection
# back to the pool. Example:
#
#   DB.transaction(rollback: :always, auto_savepoint: true) do |conn|
#     DB.temporarily_release_connection(conn) do
#       # Other threads can operate on connection safely inside the transaction
#       yield
#     end
#   end
#
# For sharded connection pools, the second argument to +temporarily_release_connection+
# is respected, and specifies the server on which to temporarily release the connection.
#
# The temporarily_release_connection extension is only supported with the
# threaded and timed_queue connection pools that ship with Sequel (and the sharded
# versions of each).  To make sure that same connection object can be reacquired, it
# is only supported if the maximum connection pool size is 1, so set the Database
# :max_connections option to 1 if you plan to use this extension.
#
# If the +temporarily_release_connection+ method cannot reacquire the same connection
# it released to the pool, it will raise a Sequel::UnableToReacquireConnectionError
# exception.  This should only happen if the connection has been disconnected
# while it was temporarily released.  If this error is raised, Database#transaction
# will not rollback the transaction, since the connection object is likely no longer
# valid, and on poorly written database drivers, that could cause the process to crash.
#
# Related modules: Sequel::TemporarilyReleaseConnection,
# Sequel::UnableToReacquireConnectionError

#
module Sequel
  # Error class raised if the connection pool does not provide the same connection
  # object when checking a temporarily released connection out.
  class UnableToReacquireConnectionError < Error
  end

  module TemporarilyReleaseConnection
    module DatabaseMethods
      # Temporarily release the connection back to the connection pool for the
      # duration of the block.
      def temporarily_release_connection(conn, server=:default, &block)
        pool.temporarily_release_connection(conn, server, &block)
      end

      private

      # Do nothing if UnableToReacquireConnectionError is raised, as it is
      # likely the connection is not in a usable state.
      def rollback_transaction(conn, opts)
        return if UnableToReacquireConnectionError === $!
        super
      end
    end

    module PoolMethods
      # Temporarily release a currently checked out connection, then yield to the block. Reacquire the same
      # connection upon the exit of the block.
      def temporarily_release_connection(conn, server)
        t = Sequel.current
        raise Error, "connection not currently checked out" unless conn.equal?(trc_owned_connection(t, server))

        begin
          trc_release(t, conn, server)
          yield
        ensure
          c = trc_acquire(t, server)
          unless conn.equal?(c)
            raise UnableToReacquireConnectionError, "reacquired connection not the same as initial connection"
          end
        end
      end
    end

    module TimedQueue
      private

      def trc_owned_connection(t, server)
        owned_connection(t)
      end

      def trc_release(t, conn, server)
        release(t)
      end

      def trc_acquire(t, server)
        acquire(t)
      end
    end

    module ShardedTimedQueue
      # Normalize the server name for sharded connection pools
      def temporarily_release_connection(conn, server)
        server = pick_server(server)
        super
      end

      private

      def trc_owned_connection(t, server)
        owned_connection(t, server)
      end

      def trc_release(t, conn, server)
        release(t, conn, server)
      end

      def trc_acquire(t, server)
        acquire(t, server)
      end
    end

    module ThreadedBase
      private

      def trc_release(t, conn, server)
        sync{super}
      end
    end

    module Threaded
      include TimedQueue
      include ThreadedBase
    end

    module ShardedThreaded
      include ShardedTimedQueue
      include ThreadedBase
    end
  end

  trc = TemporarilyReleaseConnection
  trc_map = {
    :threaded => trc::Threaded,
    :sharded_threaded => trc::ShardedThreaded,
    :timed_queue => trc::TimedQueue,
    :sharded_timed_queue => trc::ShardedTimedQueue,
  }.freeze

  Database.register_extension(:temporarily_release_connection) do |db|
    unless pool_mod = trc_map[db.pool.pool_type]
      raise(Error, "temporarily_release_connection extension not supported for connection pool type #{db.pool.pool_type}")
    end

    case db.pool.pool_type
    when :threaded, :sharded_threaded
      if db.opts[:connection_handling] == :disconnect
        raise Error, "temporarily_release_connection extension not supported with connection_handling: :disconnect option"
      end
    end

    unless db.pool.max_size == 1
      raise Error, "temporarily_release_connection extension not supported unless :max_connections option is 1"
    end

    db.extend(trc::DatabaseMethods)
    db.pool.extend(trc::PoolMethods)
    db.pool.extend(pool_mod)
  end

  private_constant :TemporarilyReleaseConnection
end
