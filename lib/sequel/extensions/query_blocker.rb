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
#     ds.all                   # Exception raised
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
# Database#block_queries is useful for blocking queries inside
# the block.  However, there may be cases where you want to
# allow queries in specific places inside a block_queries block.
# You can use Database#allow_queries for that:
#
#   DB.block_queries do
#     DB.allow_queries do
#       DB[:table].all           # Query allowed
#     end
#
#     DB[:table].all           # Exception raised
#   end
#
# When mixing block_queries and allow_queries with scopes, the
# narrowest scope has priority.  So if you are blocking with
# :thread scope, and allowing with :fiber scope, queries in the
# current fiber will be allowed, but queries in different fibers of
# the current thread will be blocked.
#
# Note that this should catch all queries executed through the
# Database instance.  Whether it catches queries executed directly
# on a connection object depends on the adapter in use.
#
# Related module: Sequel::QueryBlocker

require "fiber"

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

    # If checking a connection for validity, and a BlockedQuery exception is
    # raised, treat it as a valid connection.  You cannot check whether the
    # connection is valid without issuing a query, and if queries are blocked,
    # you need to assume it is valid or assume it is not.  Since it most cases
    # it will be valid, this assumes validity.
    def valid_connection?(conn)
      super
    rescue BlockedQuery
      true
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
      b.fetch(Fiber.current) do
        b.fetch(Thread.current) do
          b.fetch(:global, false)
        end
      end
    end

    # Allow queries inside the block.  Only useful if they are already blocked
    # for the same scope. Useful for blocking queries generally, and only allowing
    # them in specific places.  Takes the same :scope option as #block_queries.
    def allow_queries(opts=OPTS, &block)
      _allow_or_block_queries(false, opts, &block)
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
    def block_queries(opts=OPTS, &block)
      _allow_or_block_queries(true, opts, &block)
    end
    
    private

    # Internals of block_queries and allow_queries.
    def _allow_or_block_queries(value, opts)
      scope = query_blocker_scope(opts)
      prev_value = nil
      scopes = @blocked_query_scopes

      begin
        Sequel.synchronize do
          prev_value = scopes[scope]
          scopes[scope] = value
        end

        yield
      ensure
        Sequel.synchronize do
          if prev_value.nil?
            scopes.delete(scope)
          else
            scopes[scope] = prev_value
          end
        end
      end
    end

    # The scope for the query block, either :global, or a Thread or Fiber instance.
    def query_blocker_scope(opts)
      case scope = opts[:scope]
      when nil
        :global
      when :global, Thread, Fiber
        scope
      when :thread
        Thread.current
      when :fiber
        Fiber.current
      else
        raise Sequel::Error, "invalid scope given to block_queries: #{scope.inspect}"
      end
    end

    # Raise a BlockQuery exception if queries are currently blocked.
    def check_blocked_queries!
      raise BlockedQuery, "cannot execute query inside a block_queries block" if block_queries?
    end
  end

  Database.register_extension(:query_blocker, QueryBlocker)
end
