# frozen-string-literal: true
#
# The connection_checkout_event_callback extension modifies a database's
# connection pool to allow for a checkout event callback. This callback is
# called with the following arguments:
#
# :immediately_available :: Connection immediately available and returned
# :not_immediately_available :: Connection not immediately available
# :new_connection :: New connection created and returned
# Float :: Number of seconds waiting to acquire a connection
#
# This is a low-level extension that allows for building telemetry
# information. It doesn't implement any telemetry reporting itself. The
# main reason for recording this information is to use it to determine the
# appropriate size for the connection pool. Having too large a connection
# pool can waste resources, while having too small a connection pool can
# result in substantial time to check out a connection. In general, you
# want to use as small a pool as possible while keeping the time to
# checkout a connection low.
#
# To use the connection checkout event callback, you must first load the
# extension:
#
#   DB.extension(:connection_checkout_event_callback)
#
# By default, an empty proc is used as the callback so that loading the
# support doesn't break anything. If you are using the extension, you
# should set the callback at some point during application startup:
#
#   DB.pool.on_checkout_event = proc do |event|
#     # ...
#   end
#
# When using the sharded connection pool, the callback is also
# passed a second argument, the requested server shard (generally a
# symbol), allowing for collection of per-shard telemetry:
#
#   DB.pool.on_checkout_event = proc do |event, server|
#     # ...
#   end
#
# Note that the callback may be called currently by multiple threads.
# You should use some form of concurrency control inside the callback,
# such as a mutex or queue.
#
# Below is a brief example of usage to determine the percentage of
# connection requests where a connection was immediately available:
#
#   mutex = Mutex.new
#   total = immediates = 0
#
#   DB.pool.on_checkout_event = proc do |event|
#     case event
#     when :immediately_available
#       mutex.synchronize do
#         total += 1
#         immediates += 1
#       end
#     when :not_immediately_available
#       mutex.synchronize do
#         total += 1
#       end
#     end
#   end
#
#   immediate_percentage = lambda do
#     mutex.synchronize do
#       100.0 * immediates / total
#     end
#   end
#   
# Note that this extension only works with the timed_queue
# and sharded_timed_queue connection pools (the default
# connection pools when using Ruby 3.2+).
#
# Related modules: Sequel::ConnectionCheckoutEventCallbacks::TimedQueue,
# Sequel::ConnectionCheckoutEventCallbacks::ShardedTimedQueue

#
module Sequel
  module ConnectionCheckoutEventCallbacks
    module TimedQueue
      # The callback that is called with connection checkout events.
      attr_accessor :on_checkout_event

      private

      def available
        conn = super
        @on_checkout_event.call(conn ? :immediately_available : :not_immediately_available)
        conn
      end

      def try_make_new
        conn = super
        @on_checkout_event.call(:new_connection) if conn
        conn
      end

      def wait_until_available
        timer = Sequel.start_timer
        conn = super
        @on_checkout_event.call(Sequel.elapsed_seconds_since(timer))
        conn
      end
    end

    module ShardedTimedQueue
      # The callback that is called with connection checkout events.
      attr_accessor :on_checkout_event

      private

      def available(queue, server)
        conn = super
        @on_checkout_event.call(conn ? :immediately_available : :not_immediately_available, server)
        conn
      end

      def try_make_new(server)
        conn = super
        @on_checkout_event.call(:new_connection, server) if conn
        conn
      end

      def wait_until_available(queue, server)
        timer = Sequel.start_timer
        conn = super
        @on_checkout_event.call(Sequel.elapsed_seconds_since(timer), server)
        conn
      end
    end
  end

  default_callback = proc{}

  Database.register_extension(:connection_checkout_event_callback) do |db|
    pool = db.pool

    case pool.pool_type
    when :timed_queue
      db.pool.extend(ConnectionCheckoutEventCallbacks::TimedQueue)
    when :sharded_timed_queue
      db.pool.extend(ConnectionCheckoutEventCallbacks::ShardedTimedQueue)
    else
      raise Error, "the connection_checkout_event_callback extension is only supported when using a timed_queue connection pool"
    end

    pool.on_checkout_event ||= default_callback
  end
end
