# frozen-string-literal: true

require 'sequel/connection_pool/threaded'

# The slowest and most advanced connection, dealing with both multi-threaded
# access and configurations with multiple shards/servers.
#
# In addition, this pool subclass also handles scheduling in-use connections
# to be removed from the pool when they are returned to it.
class Sequel::ShardedThreadedConnectionPool < Sequel::ThreadedConnectionPool
  # The following additional options are respected:
  # :servers :: A hash of servers to use.  Keys should be symbols.  If not
  #             present, will use a single :default server.
  # :servers_hash :: The base hash to use for the servers.  By default,
  #                  Sequel uses Hash.new(:default).  You can use a hash with a default proc
  #                  that raises an error if you want to catch all cases where a nonexistent
  #                  server is used.
  def initialize(db, opts = OPTS)
    super
    @available_connections = {}
    @connections_to_remove = []
    @servers = opts.fetch(:servers_hash, Hash.new(:default))

    if USE_WAITER
      @waiter = nil
      @waiters = {}
    end

    add_servers([:default])
    add_servers(opts[:servers].keys) if opts[:servers]
  end
  
  # Adds new servers to the connection pool. Primarily used in conjunction with master/slave
  # or shard configurations. Allows for dynamic expansion of the potential slaves/shards
  # at runtime. servers argument should be an array of symbols. 
  def add_servers(servers)
    sync do
      servers.each do |server|
        unless @servers.has_key?(server)
          @servers[server] = server
          @available_connections[server] = []
          @allocated[server] = {}
          @waiters[server] = ConditionVariable.new if USE_WAITER
        end
      end
    end
  end
  
  # A hash of connections currently being used for the given server, key is the
  # Thread, value is the connection.  Nonexistent servers will return nil.  Treat
  # this as read only, do not modify the resulting object.
  def allocated(server=:default)
    @allocated[server]
  end
  
  # Yield all of the available connections, and the ones currently allocated to
  # this thread.  This will not yield connections currently allocated to other
  # threads, as it is not safe to operate on them.  This holds the mutex while
  # it is yielding all of the connections, which means that until
  # the method's block returns, the pool is locked.
  def all_connections
    t = Thread.current
    sync do
      @allocated.values.each do |threads|
        threads.each do |thread, conn|
          yield conn if t == thread
        end
      end
      @available_connections.values.each{|v| v.each{|c| yield c}}
    end
  end
  
  # An array of connections opened but not currently used, for the given
  # server. Nonexistent servers will return nil. Treat this as read only, do
  # not modify the resulting object.
  def available_connections(server=:default)
    @available_connections[server]
  end
  
  # The total number of connections opened for the given server, should
  # be equal to available_connections.length + allocated.length.  Nonexistent
  # servers will return the created count of the default server.
  def size(server=:default)
    server = @servers[server]
    @allocated[server].length + @available_connections[server].length
  end
  
  # Removes all connections currently available on all servers, optionally
  # yielding each connection to the given block. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used.  If connections are being used, they are scheduled to be
  # disconnected as soon as they are returned to the pool.
  # 
  # Once a connection is requested using #hold, the connection pool
  # creates new connections to the database. Options:
  # :server :: Should be a symbol specifing the server to disconnect from,
  #            or an array of symbols to specify multiple servers.
  def disconnect(opts=OPTS)
    sync do
      (opts[:server] ? Array(opts[:server]) : @servers.keys).each do |s|
        disconnect_server(s)
      end
    end
  end
  
  # Chooses the first available connection to the given server, or if none are
  # available, creates a new connection.  Passes the connection to the supplied
  # block:
  # 
  #   pool.hold {|conn| conn.execute('DROP TABLE posts')}
  # 
  # Pool#hold is re-entrant, meaning it can be called recursively in
  # the same thread without blocking.
  #
  # If no connection is immediately available and the pool is already using the maximum
  # number of connections, Pool#hold will block until a connection
  # is available or the timeout expires.  If the timeout expires before a
  # connection can be acquired, a Sequel::PoolTimeout is 
  # raised.
  def hold(server=:default)
    server = pick_server(server)
    t = Thread.current
    if conn = owned_connection(t, server)
      return yield(conn)
    end
    begin
      conn = acquire(t, server)
      yield conn
    rescue Sequel::DatabaseDisconnectError
      sync{@connections_to_remove << conn} if conn
      raise
    ensure
      sync{release(t, conn, server)} if conn
    end
  end

  # Remove servers from the connection pool. Primarily used in conjunction with master/slave
  # or shard configurations.  Similar to disconnecting from all given servers,
  # except that after it is used, future requests for the server will use the
  # :default server instead.
  def remove_servers(servers)
    sync do
      raise(Sequel::Error, "cannot remove default server") if servers.include?(:default)
      servers.each do |server|
        if @servers.include?(server)
          disconnect_server(server)
          @waiters.delete(server) if USE_WAITER
          @available_connections.delete(server)
          @allocated.delete(server)
          @servers.delete(server)
        end
      end
    end
  end

  # Return an array of symbols for servers in the connection pool.
  def servers
    sync{@servers.keys}
  end

  def pool_type
    :sharded_threaded
  end
  
  private

  # Assigns a connection to the supplied thread for the given server, if one
  # is available. The calling code should already have the mutex when
  # calling this.
  def _acquire(thread, server)
    if conn = available(server)
      allocated(server)[thread] = conn
    end
  end
  
  if USE_WAITER
    # Assigns a connection to the supplied thread, if one
    # is available. The calling code should NOT already have the mutex when
    # calling this.
    #
    # This should return a connection is one is available within the timeout,
    # or nil if a connection could not be acquired within the timeout.
    def acquire(thread, server)
      sync do
        if conn = _acquire(thread, server)
          return conn
        end

        time = Time.now
        @waiters[server].wait(@mutex, @timeout)
        Thread.pass

        until conn = _acquire(thread, server)
          deadline ||= time + @timeout
          current_time = Time.now
          raise(::Sequel::PoolTimeout, "timeout: #{@timeout}, elapsed: #{current_time - time}") if current_time > deadline
          # :nocov:
          # It's difficult to get to this point, it can only happen if there is a race condition
          # where a connection cannot be acquired even after the thread is signalled by the condition
          # variable that a connection is ready.
          @waiters[server].wait(@mutex, deadline - current_time)
          Thread.pass
          # :nocov:
        end

        conn
      end
    end
  else
    # :nocov:
    def acquire(thread, server)
      unless conn = sync{_acquire(thread, server)}
        time = Time.now
        timeout = time + @timeout
        sleep_time = @sleep_time
        sleep sleep_time
        until conn = sync{_acquire(thread, server)}
          raise(::Sequel::PoolTimeout, "timeout: #{@timeout}, elapsed: #{Time.now - time}") if Time.now > timeout
          sleep sleep_time
        end
      end
      conn
    end
    # :nocov:
  end

  # Returns an available connection to the given server. If no connection is
  # available, tries to create a new connection. The calling code should already
  # have the mutex before calling this.
  def available(server)
    next_available(server) || make_new(server)
  end

  # Return a connection to the pool of available connections for the server,
  # returns the connection. The calling code should already have the mutex
  # before calling this.
  def checkin_connection(server, conn)
    available_connections(server) << conn
    if USE_WAITER
      @waiters[server].signal
      Thread.pass
    end
    conn
  end

  # Disconnect from the given server.  Disconnects available connections
  # immediately, and schedules currently allocated connections for disconnection
  # as soon as they are returned to the pool. The calling code should already
  # have the mutex before calling this.
  def disconnect_server(server)
    if conns = available_connections(server)
      conns.each{|conn| db.disconnect_connection(conn)}
      conns.clear
    end
    @connections_to_remove.concat(allocated(server).values)
  end

  # Creates a new connection to the given server if the size of the pool for
  # the server is less than the maximum size of the pool. The calling code
  # should already have the mutex before calling this.
  def make_new(server)
    if (n = size(server)) >= @max_size
      allocated(server).to_a.each{|t, c| release(t, c, server) unless t.alive?}
      n = nil
    end
    default_make_new(server) if (n || size(server)) < @max_size
  end

  # Return the next available connection in the pool for the given server, or nil
  # if there is not currently an available connection for the server.
  # The calling code should already have the mutex before calling this.
  def next_available(server)
    case @connection_handling
    when :stack
      available_connections(server).pop
    else
      available_connections(server).shift
    end
  end
  
  # Returns the connection owned by the supplied thread for the given server,
  # if any. The calling code should NOT already have the mutex before calling this.
  def owned_connection(thread, server)
    sync{@allocated[server][thread]}
  end

  # If the server given is in the hash, return it, otherwise, return the default server.
  def pick_server(server)
    sync{@servers[server]}
  end
  
  # Create the maximum number of connections to each server immediately.
  def preconnect
    servers.each{|s| (max_size - size(s)).times{checkin_connection(s, make_new(s))}}
  end
  
  # Releases the connection assigned to the supplied thread and server. If the
  # server or connection given is scheduled for disconnection, remove the
  # connection instead of releasing it back to the pool.
  # The calling code should already have the mutex before calling this.
  def release(thread, conn, server)
    if @connections_to_remove.include?(conn)
      remove(thread, conn, server)
    else
      conn = allocated(server).delete(thread)

      if @connection_handling == :disconnect
        db.disconnect_connection(conn)
      else
        checkin_connection(server, conn)
      end
    end
  end

  # Removes the currently allocated connection from the connection pool. The
  # calling code should already have the mutex before calling this.
  def remove(thread, conn, server)
    @connections_to_remove.delete(conn)
    allocated(server).delete(thread) if @servers.include?(server)
    db.disconnect_connection(conn)
  end
  
  CONNECTION_POOL_MAP[[false, true]] = self
end
