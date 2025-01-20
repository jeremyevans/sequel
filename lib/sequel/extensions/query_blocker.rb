# frozen-string-literal: true
#
# The query_blocker extension adds Database#block_queries.
# Inside the block passed to #block_queries, any attempts to
# execute a query/statement on the database will raise a
# Sequel::QueryBlocker::BlockedQuery exception.
#
#   DB.extension :query_blocker
#   DB.block_queries do
#     ds = DB[:table]          # No exception
#     ds = ds.where(column: 1) # No exception
#     ds.all                   # Attempts query, exception raised
#   end
#
# To handle concurrency, you can pass a :scope option:
#
#   # Current Thread
#   DB.block_queries(scope: :thread){}
#
#   # Current Fiber
#   DB.block_queries(scope: :fiber){}
#
#   # Specific Thread
#   DB.block_queries(scope: Thread.current){}
#
#   # Specific Fiber
#   DB.block_queries(scope: Fiber.current){}
#
# Note that this should catch all queries executed through the
# Database instance.  Whether it catches queries executed directly
# on a connection object depends on the adapter in use.
#
# Related module: Sequel::QueryBlocker

# :nocov:
require "fiber" if RUBY_VERSION <= "2.7"
# :nocov:

#
module Sequel
  module QueryBlocker
    # Exception class raised if there is an attempt to execute a 
    # query/statement on the database inside a block passed to
    # block_queries.
    class BlockedQuery < Sequel::Error
    end

    def self.extended(db)
      db.instance_exec do
        @blocked_query_scopes ||= {}
      end
    end

    # Check whether queries are blocked before executing them.
    def log_connection_yield(sql, conn, args=nil)
      # All database adapters should be calling this method around
      # query execution (otherwise the queries would not get logged),
      # ensuring the blocking is checked.  Any database adapter issuing
      # a query without calling this method is considered buggy.
      check_blocked_queries!
      super
    end

    # Whether queries are currently blocked.
    def block_queries?
      b = @blocked_query_scopes
      Sequel.synchronize{b[:global] || b[Thread.current] || b[Fiber.current]} || false
    end

    # Reject (raise an BlockedQuery exception) if there is an attempt to execute
    # a query/statement inside the block.
    #
    # The :scope option indicates which queries are rejected inside the block:
    #
    # :global :: This is the default, and rejects all queries.
    # :thread :: Reject all queries in the current thread.
    # :fiber :: Reject all queries in the current fiber.
    # Thread :: Reject all queries in the given thread.
    # Fiber :: Reject all queries in the given fiber.
    def block_queries(opts=OPTS)
      case scope = opts[:scope]
      when nil
        scope = :global
      when :global
        #  nothing
      when :thread
        scope = Thread.current
      when :fiber
        scope = Fiber.current
      when Thread, Fiber
        # nothing
      else
        raise Sequel::Error, "invalid scope given to block_queries: #{scope.inspect}"
      end

      prev_value = nil
      scopes = @blocked_query_scopes

      begin
        Sequel.synchronize do
          prev_value = scopes[scope]
          scopes[scope] = true
        end

        yield
      ensure
        Sequel.synchronize do
          if prev_value
            scopes[scope] = prev_value
          else
            scopes.delete(scope)
          end
        end
      end
    end
    
    private

    # Raise a BlockQuery exception if queries are currently blocked.
    def check_blocked_queries!
      raise BlockedQuery, "cannot execute query inside a block_queries block" if block_queries?
    end
  end

  Database.register_extension(:query_blocker, QueryBlocker)
end
