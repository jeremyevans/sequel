# frozen-string-literal: true

# A connection pool allowing multi-threaded access to a pool of connections.
# This is the default connection pool used by Sequel.
class Sequel::ThreadedConnectionPool < Sequel::ConnectionPool
  # Whether or not a ConditionVariable should be used to wait for connections.
  # True except on ruby 1.8, where ConditionVariable#wait does not support a
  # timeout.
  unless defined?(USE_WAITER)
    USE_WAITER = RUBY_VERSION >= '1.9'
  end

  # The maximum number of connections this pool will create (per shard/server
  # if sharding).
  attr_reader :max_size
  
  # An array of connections that are available for use by the pool.
  attr_reader :available_connections
  
  # A hash with thread keys and connection values for currently allocated
  # connections.
  attr_reader :allocated

  # The following additional options are respected:
  # * :connection_handling - Set how to handle available connections.  By default,
  #   uses a a queue for fairness.  Can be set to :stack to use a stack, which may
  #   offer better performance.
  # * :max_connections - The maximum number of connections the connection pool
  #   will open (default 4)
  # * :pool_sleep_time - The amount of time to sleep before attempting to acquire
  #   a connection again, only used on ruby 1.8. (default 0.001)
  # * :pool_timeout - The amount of seconds to wait to acquire a connection
  #   before raising a PoolTimeoutError (default 5)
  def initialize(db, opts = OPTS)
    super
    @max_size = Integer(opts[:max_connections] || 4)
    raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1
    @mutex = Mutex.new  
    @connection_handling = opts[:connection_handling]
    @available_connections = []
    @allocated = {}
    @timeout = Float(opts[:pool_timeout] || 5)

    if USE_WAITER
      @waiter = ConditionVariable.new
    else
      # :nocov:
      @sleep_time = Float(opts[:pool_sleep_time] || 0.001)
      # :nocov:
    end
  end
  
  # Yield all of the available connections, and the one currently allocated to
  # this thread.  This will not yield connections currently allocated to other
  # threads, as it is not safe to operate on them.  This holds the mutex while
  # it is yielding all of the available connections, which means that until
  # the method's block returns, the pool is locked.
  def all_connections
    hold do |c|
      sync do
        yield c
        @available_connections.each{|conn| yield conn}
      end
    end
  end
  
  # Removes all connections currently available, optionally
  # yielding each connection to the given block. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used.  If you want to be able to disconnect connections that are
  # currently in use, use the ShardedThreadedConnectionPool, which can do that.
  # This connection pool does not, for performance reasons. To use the sharded pool,
  # pass the <tt>:servers=>{}</tt> option when connecting to the database.
  # 
  # Once a connection is requested using #hold, the connection pool
  # creates new connections to the database.
  def disconnect(opts=OPTS)
    conns = nil
    sync do
      conns = @available_connections.dup
      @available_connections.clear
    end
    conns.each{|conn| disconnect_connection(conn)}
  end
  
  # Chooses the first available connection, or if none are
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
  def hold(server=nil)
    t = Thread.current
    if conn = owned_connection(t)
      return yield(conn)
    end
    begin
      conn = acquire(t)
      yield conn
    rescue Sequel::DatabaseDisconnectError, *@error_classes => e
      if disconnect_error?(e)
        oconn = conn
        conn = nil
        disconnect_connection(oconn) if oconn
        @allocated.delete(t)
      end
      raise
    ensure
      sync{release(t)} if conn
    end
  end

  def pool_type
    :threaded
  end
  
  # The total number of connections opened, either available or allocated.
  # This may not be completely accurate as it isn't protected by the mutex.
  def size
    @allocated.length + @available_connections.length
  end
  
  private

  # Assigns a connection to the supplied thread, if one
  # is available. The calling code should already have the mutex when
  # calling this.
  def _acquire(thread)
    if conn = available
      @allocated[thread] = conn
    end
  end
  
  if USE_WAITER
    # Assigns a connection to the supplied thread, if one
    # is available. The calling code should NOT already have the mutex when
    # calling this.
    #
    # This should return a connection is one is available within the timeout,
    # or nil if a connection could not be acquired within the timeout.
    def acquire(thread)
      sync do
        if conn = _acquire(thread)
          return conn
        end

        time = Time.now
        @waiter.wait(@mutex, @timeout)

        # Not sure why this is helpful, but calling Thread.pass after conditional
        # variable access dramatically increases reliability when under heavy
        # resource contention (almost eliminating timeouts), at a small cost to
        # runtime performance.
        Thread.pass

        until conn = _acquire(thread)
          deadline ||= time + @timeout
          current_time = Time.now
          raise_pool_timeout(current_time - time) if current_time > deadline
          # :nocov:
          # It's difficult to get to this point, it can only happen if there is a race condition
          # where a connection cannot be acquired even after the thread is signalled by the condition
          @waiter.wait(@mutex, deadline - current_time)
          Thread.pass
          # :nocov:
        end

        conn
      end
    end
  else
    # :nocov:
    def acquire(thread)
      unless conn = sync{_acquire(thread)}
        time = Time.now
        timeout = time + @timeout
        sleep_time = @sleep_time
        sleep sleep_time
        until conn = sync{_acquire(thread)}
          raise_pool_timeout(Time.now - time) if Time.now > timeout
          sleep sleep_time
        end
      end
      conn
    end
    # :nocov:
  end

  # Returns an available connection. If no connection is
  # available, tries to create a new connection. The calling code should already
  # have the mutex before calling this.
  def available
    next_available || make_new(DEFAULT_SERVER)
  end

  # Return a connection to the pool of available connections, returns the connection.
  # The calling code should already have the mutex before calling this.
  def checkin_connection(conn)
    @available_connections << conn
    if USE_WAITER
      @waiter.signal
      Thread.pass
    end
    conn
  end

  unless method_defined?(:default_make_new)
    # Alias the default make_new method, so subclasses can call it directly.
    alias default_make_new make_new
  end
  
  # Creates a new connection to the given server if the size of the pool for
  # the server is less than the maximum size of the pool. The calling code
  # should already have the mutex before calling this.
  def make_new(server)
    if (n = size) >= @max_size
      @allocated.keys.each{|t| release(t) unless t.alive?}
      n = nil
    end
    super if (n || size) < @max_size
  end

  # Return the next available connection in the pool, or nil if there
  # is not currently an available connection.  The calling code should already
  # have the mutex before calling this.
  def next_available
    case @connection_handling
    when :stack
      @available_connections.pop
    else
      @available_connections.shift
    end
  end

  # Returns the connection owned by the supplied thread,
  # if any. The calling code should NOT already have the mutex before calling this.
  def owned_connection(thread)
    sync{@allocated[thread]}
  end
  
  # Create the maximum number of connections immediately.
  def preconnect(concurrent = false)
    enum = (max_size - size).times

    if concurrent
      enum.map{Thread.new{make_new(nil)}}.map(&:join).each{|t| checkin_connection(t.value)}
    else
      enum.each{checkin_connection(make_new(nil))}
    end
  end

  # Raise a PoolTimeout error showing the current timeout, the elapsed time, and the
  # database's name (if any).
  def raise_pool_timeout(elapsed)
    name = db.opts[:name]
    raise ::Sequel::PoolTimeout, "timeout: #{@timeout}, elapsed: #{elapsed}#{", database name: #{name}" if name}"
  end
  
  # Releases the connection assigned to the supplied thread back to the pool.
  # The calling code should already have the mutex before calling this.
  def release(thread)
    conn = @allocated.delete(thread)

    if @connection_handling == :disconnect
      disconnect_connection(conn)
    else
      checkin_connection(conn)
    end
  end

  # Yield to the block while inside the mutex. The calling code should NOT
  # already have the mutex before calling this.
  def sync
    @mutex.synchronize{yield}
  end
  
  CONNECTION_POOL_MAP[[false, false]] = self
end
