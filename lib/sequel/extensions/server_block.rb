# frozen-string-literal: true
#
# The server_block extension adds the Database#with_server method, which takes a shard
# argument and a block, and makes it so that access inside the block will use the
# specified shard by default.
#
# First, you need to enable it on the database object:
#
#   DB.extension :server_block
# 
# Then you can call with_server:
#
#   DB.with_server(:shard1) do
#     DB[:a].all # Uses shard1
#     DB[:a].server(:shard2).all # Uses shard2
#   end
#   DB[:a].all # Uses default
#
# You can even nest calls to with_server:
#
#   DB.with_server(:shard1) do
#     DB[:a].all # Uses shard1
#     DB.with_server(:shard2) do
#       DB[:a].all # Uses shard2
#     end
#     DB[:a].all # Uses shard1
#   end
#   DB[:a].all # Uses default
# 
# Note that if you pass the nil, :default, or :read_only server/shard
# names to Dataset#server inside a with_server block, they will be
# ignored and the server/shard given to with_server will be used:
#
#   DB.with_server(:shard1) do
#     DB[:a].all # Uses shard1
#     DB[:a].server(:shard2).all # Uses shard2
#     DB[:a].server(nil).all # Uses shard1
#     DB[:a].server(:default).all # Uses shard1
#     DB[:a].server(:read_only).all # Uses shard1
#   end
#
# Related modules: Sequel::ServerBlock, Sequel::UnthreadedServerBlock,
# Sequel::ThreadedServerBlock

#
module Sequel
  module ServerBlock
    # Enable the server block on the connection pool, choosing the correct
    # extension depending on whether the connection pool is threaded or not.
    # Also defines the with_server method on the receiver for easy use.
    def self.extended(db)
      pool = db.pool
      if defined?(ShardedThreadedConnectionPool) && pool.is_a?(ShardedThreadedConnectionPool)
        pool.extend(ThreadedServerBlock)
        pool.instance_variable_set(:@default_servers, {})
      else
        pool.extend(UnthreadedServerBlock)
        pool.instance_variable_set(:@default_servers, [])
      end
    end

    # Delegate to the connection pool
    def with_server(server, &block)
      pool.with_server(server, &block)
    end
  end

  # Adds with_server support for the sharded single connection pool.
  module UnthreadedServerBlock
    # Set a default server/shard to use inside the block.
    def with_server(server)
      begin
        set_default_server(server)
        yield
      ensure
        clear_default_server
      end
    end

    private

    # Make the given server the new default server.
    def set_default_server(server)
      @default_servers << server
    end

    # Remove the current default server, restoring the
    # previous default server.
    def clear_default_server
      @default_servers.pop
    end

    # Use the server given to with_server if appropriate.
    def pick_server(server)
      if @default_servers.empty?
        super
      else
        case server
        when :default, nil, :read_only
          @default_servers.last
        else
          super
        end
      end
    end
  end

  # Adds with_server support for the sharded threaded connection pool.
  module ThreadedServerBlock
    # Set a default server/shard to use inside the block for the current
    # thread.
    def with_server(server)
      begin
        set_default_server(server)
        yield
      ensure
        clear_default_server
      end
    end

    private

    # Make the given server the new default server for the current thread.
    def set_default_server(server)
      sync{(@default_servers[Thread.current] ||= [])} << server
    end

    # Remove the current default server for the current thread, restoring the
    # previous default server.
    def clear_default_server
      t = Thread.current
      a = sync{@default_servers[t]}
      a.pop
      sync{@default_servers.delete(t)} if a.empty?
    end

    # Use the server given to with_server for the given thread, if appropriate.
    def pick_server(server)
      a = sync{@default_servers[Thread.current]}
      if !a || a.empty?
        super
      else
        case server
        when :default, nil, :read_only
          a.last
        else
          super
        end
      end
    end
  end

  Database.register_extension(:server_block, ServerBlock)
end
