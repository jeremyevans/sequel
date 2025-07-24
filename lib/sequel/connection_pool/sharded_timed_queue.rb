# frozen-string-literal: true

# :nocov:
raise LoadError, "Sequel::ShardedTimedQueueConnectionPool is only available on Ruby 3.2+" unless RUBY_VERSION >= '3.2'
# :nocov:

# A connection pool allowing multi-threaded access to a sharded pool of connections,
# using a timed queue (only available in Ruby 3.2+).
class Sequel::ShardedTimedQueueConnectionPool < Sequel::ConnectionPool
  # The maximum number of connections this pool will create per shard.
  attr_reader :max_size

  # The following additional options are respected:
  # :max_connections :: The maximum number of connections the connection pool
  #                     will open (default 4)
  # :pool_timeout :: The amount of seconds to wait to acquire a connection
  #                  before raising a PoolTimeout (default 5)
  # :servers :: A hash of servers to use.  Keys should be symbols.  If not
  #             present, will use a single :default server.
  # :servers_hash :: The base hash to use for the servers.  By default,
  #                  Sequel uses Hash.new(:default).  You can use a hash with a default proc
  #                  that raises an error if you want to catch all cases where a nonexistent
  #                  server is used.
  def initialize(db, opts = OPTS)
    super

    @max_size = Integer(opts[:max_connections] || 4)
    raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1
    @mutex = Mutex.new  
    @timeout = Float(opts[:pool_timeout] || 5)

    @allocated = {}
    @sizes = {}
    @queues = {}
    @servers = opts.fetch(:servers_hash, Hash.new(:default))

    add_servers([:default])
    add_servers(opts[:servers].keys) if opts[:servers]
  end

  # Adds new servers to the connection pool.  Allows for dynamic expansion of the potential replicas/shards
  # at runtime. +servers+ argument should be an array of symbols. 
  def add_servers(servers)
    sync do
      servers.each do |server|
        next if @servers.has_key?(server)

        @servers[server] = server
        @sizes[server] = 0
        @queues[server] = Queue.new
        (@allocated[server] = {}).compare_by_identity
      end
    end
    nil
  end
  
  # Yield all of the available connections, and the one currently allocated to
  # this thread (if one is allocated).  This will not yield connections currently
  # allocated to other threads, as it is not safe to operate on them.
  def all_connections
    thread = Sequel.current
    sync{@queues.to_a}.each do |server, queue|
      if conn = owned_connection(thread, server)
        yield conn
      end

      # Use a hash to record all connections already seen.  As soon as we
      # come across a connection we've already seen, we stop the loop.
      conns = {}
      conns.compare_by_identity
      while true
        conn = nil
        begin
          break unless (conn = queue.pop(timeout: 0)) && !conns[conn]
          conns[conn] = true
          yield conn
        ensure
          queue.push(conn) if conn
        end
      end
    end

    nil
  end
  
  # Removes all connections currently in the pool's queue. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used.
  # 
  # Once a connection is requested using #hold, the connection pool
  # creates new connections to the database.
  #
  # If the :server option is provided, it should be a symbol or array of symbols,
  # and then the method will only disconnect connectsion from those specified shards.
  def disconnect(opts=OPTS)
    (opts[:server] ? Array(opts[:server]) : sync{@servers.keys}).each do |server|
      raise Sequel::Error, "invalid server" unless queue = sync{@queues[server]}
      while conn = queue.pop(timeout: 0)
        disconnect_pool_connection(conn, server)
      end
      fill_queue(server)
    end
    nil
  end

  # Chooses the first available connection for the given server, or if none are
  # available, creates a new connection.  Passes the connection to the supplied
  # block:
  # 
  #   pool.hold(:server1) {|conn| conn.execute('DROP TABLE posts')}
  # 
  # Pool#hold is re-entrant, meaning it can be called recursively in
  # the same thread without blocking.
  #
  # If no connection is immediately available and the pool is already using the maximum
  # number of connections, Pool#hold will block until a connection
  # is available or the timeout expires.  If the timeout expires before a
  # connection can be acquired, a Sequel::PoolTimeout is raised.
  def hold(server=:default)
    server = pick_server(server)
    t = Sequel.current
    if conn = owned_connection(t, server)
      return yield(conn)
    end

    begin
      conn = acquire(t, server)
      yield conn
    rescue Sequel::DatabaseDisconnectError, *@error_classes => e
      if disconnect_error?(e)
        oconn = conn
        conn = nil
        disconnect_pool_connection(oconn, server) if oconn
        sync{@allocated[server].delete(t)}
        fill_queue(server)
      end
      raise
    ensure
      release(t, conn, server) if conn
    end
  end

  # The number of threads waiting to check out a connection for the given
  # server.
  def num_waiting(server=:default)
    @queues[pick_server(server)].num_waiting
  end

  # The total number of connections in the pool. Using a non-existant server will return nil.
  def size(server=:default)
    sync{@sizes[server]}
  end
  
  # Remove servers from the connection pool. Similar to disconnecting from all given servers,
  # except that after it is used, future requests for the servers will use the
  # :default server instead.
  #
  # Note that an error will be raised if there are any connections currently checked
  # out for the given servers.
  def remove_servers(servers)
    conns = []
    raise(Sequel::Error, "cannot remove default server") if servers.include?(:default)

    sync do
      servers.each do |server|
        next unless @servers.has_key?(server)

        queue = @queues[server]

        while conn = queue.pop(timeout: 0)
          @sizes[server] -= 1
          conns << conn
        end

        unless @sizes[server] == 0
          raise Sequel::Error, "cannot remove server #{server} as it has allocated connections"
        end

        @servers.delete(server)
        @sizes.delete(server)
        @queues.delete(server)
        @allocated.delete(server)
      end
    end

    nil
  ensure
    disconnect_connections(conns)
  end

  # Return an array of symbols for servers in the connection pool.
  def servers
    sync{@servers.keys}
  end

  def pool_type
    :sharded_timed_queue
  end
  
  private

  # Create a new connection, after the pool's current size has already
  # been updated to account for the new connection.  If there is an exception
  # when creating the connection, decrement the current size.
  #
  # This should only be called after can_make_new?.  If there is an exception
  # between when can_make_new? is called and when preallocated_make_new
  # is called, it has the effect of reducing the maximum size of the
  # connection pool by 1, since the current size of the pool will show a
  # higher number than the number of connections allocated or
  # in the queue.
  #
  # Calling code should not have the mutex when calling this.
  def preallocated_make_new(server)
    make_new(server)
  rescue Exception
    sync{@sizes[server] -= 1}
    raise
  end

  # Disconnect all available connections immediately, and schedule currently allocated connections for disconnection
  # as soon as they are returned to the pool. The calling code should NOT
  # have the mutex before calling this.
  def disconnect_connections(conns)
    conns.each{|conn| disconnect_connection(conn)}
  end

  # Decrement the current size of the pool for the server when disconnecting connections.
  #
  # Calling code should not have the mutex when calling this.
  def disconnect_pool_connection(conn, server)
    sync{@sizes[server] -= 1}
    disconnect_connection(conn)
  end

  # If there are any threads waiting on the queue, try to create
  # new connections in a separate thread if the pool is not yet at the
  # maximum size.
  #
  # The reason for this method is to handle cases where acquire
  # could not retrieve a connection immediately, and the pool
  # was already at the maximum size.  In that case, the acquire will
  # wait on the queue until the timeout.  This method is called
  # after disconnecting to potentially add new connections to the
  # pool, so the threads that are currently waiting for connections
  # do not timeout after the pool is no longer full.
  def fill_queue(server)
    queue = sync{@queues[server]}
    if queue.num_waiting > 0
      Thread.new do
        while queue.num_waiting > 0 && (conn = try_make_new(server))
          queue.push(conn)
        end
      end
    end
  end

  # Whether the given size is less than the maximum size of the pool.
  # In that case, the pool's current size is incremented.  If this
  # method returns true, space in the pool for the connection is
  # preallocated, and preallocated_make_new should be called to
  # create the connection.
  #
  # Calling code should have the mutex when calling this.
  def can_make_new?(server, current_size)
    if @max_size > current_size
      @sizes[server] += 1
    end
  end

  # Try to make a new connection if there is space in the pool.
  # If the pool is already full, look for dead threads/fibers and
  # disconnect the related connections.
  #
  # Calling code should not have the mutex when calling this.
  def try_make_new(server)
    return preallocated_make_new(server) if sync{can_make_new?(server, @sizes[server])}

    to_disconnect = nil
    do_make_new = false

    sync do
      current_size = @sizes[server]
      alloc = @allocated[server]
      alloc.keys.each do |t|
        unless t.alive?
          (to_disconnect ||= []) << alloc.delete(t)
          current_size -= 1
        end
      end
    
      do_make_new = true if can_make_new?(server, current_size)
    end

    begin
      preallocated_make_new(server) if do_make_new
    ensure
      if to_disconnect
        to_disconnect.each{|conn| disconnect_pool_connection(conn, server)}
        fill_queue(server)
      end
    end
  end
  
  # Assigns a connection to the supplied thread, if one
  # is available.
  #
  # This should return a connection if one is available within the timeout,
  # or raise PoolTimeout if a connection could not be acquired within the timeout.
  #
  # Calling code should not have the mutex when calling this.
  def acquire(thread, server)
    queue = sync{@queues[server]}
    if conn = queue.pop(timeout: 0) || try_make_new(server) || queue.pop(timeout: @timeout)
      sync{@allocated[server][thread] = conn}
    else
      name = db.opts[:name]
      raise ::Sequel::PoolTimeout, "timeout: #{@timeout}, server: #{server}#{", database name: #{name}" if name}"
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
  
  # Create the maximum number of connections immediately. This should not be called
  # with a true argument unless no code is currently operating on the database.
  #
  # Calling code should not have the mutex when calling this.
  def preconnect(concurrent = false)
    conn_servers = sync{@servers.keys}.map!{|s| Array.new(@max_size - @sizes[s], s)}.flatten!

    if concurrent
      conn_servers.map! do |server|
        queue = sync{@queues[server]}
        Thread.new do 
          if conn = try_make_new(server)
            queue.push(conn)
          end
        end
      end.each(&:value)
    else
      conn_servers.each do |server|
        if conn = try_make_new(server)
          sync{@queues[server]}.push(conn)
        end
      end
    end

    nil
  end

  # Releases the connection assigned to the supplied thread back to the pool.
  #
  # Calling code should not have the mutex when calling this.
  def release(thread, _, server)
    checkin_connection(sync{@allocated[server].delete(thread)}, server)
    nil
  end

  # Adds a connection to the queue of available connections, returns the connection.
  def checkin_connection(conn, server)
    sync{@queues[server]}.push(conn)
    conn
  end

  # Yield to the block while inside the mutex.
  #
  # Calling code should not have the mutex when calling this.
  def sync
    @mutex.synchronize{yield}
  end
end
