# frozen-string-literal: true

# :nocov:
raise LoadError, "Sequel::TimedQueueConnectionPool is only available on Ruby 3.2+" unless RUBY_VERSION >= '3.2'
# :nocov:

# A connection pool allowing multi-threaded access to a pool of connections,
# using a timed queue (only available in Ruby 3.2+).
class Sequel::TimedQueueConnectionPool < Sequel::ConnectionPool
  # The maximum number of connections this pool will create.
  attr_reader :max_size
  
  # The following additional options are respected:
  # :max_connections :: The maximum number of connections the connection pool
  #                     will open (default 4)
  # :pool_timeout :: The amount of seconds to wait to acquire a connection
  #                  before raising a PoolTimeout (default 5)
  def initialize(db, opts = OPTS)
    super
    @max_size = Integer(opts[:max_connections] || 4)
    raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1
    @mutex = Mutex.new  
    # Size inside array so this still works while the pool is frozen.
    @size = [0]
    @allocated = {}
    @allocated.compare_by_identity
    @timeout = Float(opts[:pool_timeout] || 5)
    @queue = Queue.new
  end

  # Yield all of the available connections, and the one currently allocated to
  # this thread.  This will not yield connections currently allocated to other
  # threads, as it is not safe to operate on them.
  def all_connections
    hold do |conn|
      yield conn

      # Use a hash to record all connections already seen.  As soon as we
      # come across a connection we've already seen, we stop the loop.
      conns = {}
      conns.compare_by_identity
      while true
        conn = nil
        begin
          break unless (conn = @queue.pop(timeout: 0)) && !conns[conn]
          conns[conn] = true
          yield conn
        ensure
          @queue.push(conn) if conn
        end
      end
    end
  end
  
  # Removes all connections currently in the pool's queue. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used.
  # 
  # Once a connection is requested using #hold, the connection pool
  # creates new connections to the database.
  def disconnect(opts=OPTS)
    while conn = @queue.pop(timeout: 0)
      disconnect_connection(conn)
    end
    fill_queue
    nil
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
  # connection can be acquired, a Sequel::PoolTimeout is raised.
  def hold(server=nil)
    t = Sequel.current
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
        sync{@allocated.delete(t)}
        fill_queue
      end
      raise
    ensure
      release(t) if conn
    end
  end

  # The number of threads waiting to check out a connection.
  def num_waiting(_server=:default)
    @queue.num_waiting
  end

  def pool_type
    :timed_queue
  end
  
  # The total number of connections in the pool.
  def size
    sync{@size[0]}
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
  def preallocated_make_new
    make_new(:default)
  rescue Exception
    sync{@size[0] -= 1}
    raise
  end

  # Decrement the current size of the pool when disconnecting connections.
  #
  # Calling code should not have the mutex when calling this.
  def disconnect_connection(conn)
    sync{@size[0] -= 1}
    super
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
  def fill_queue
    if @queue.num_waiting > 0
      Thread.new do
        while @queue.num_waiting > 0 && (conn = try_make_new)
          @queue.push(conn)
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
  def can_make_new?(current_size)
    if @max_size > current_size
      @size[0] += 1
    end
  end

  # Try to make a new connection if there is space in the pool.
  # If the pool is already full, look for dead threads/fibers and
  # disconnect the related connections.
  #
  # Calling code should not have the mutex when calling this.
  def try_make_new
    return preallocated_make_new if sync{can_make_new?(@size[0])}

    to_disconnect = nil
    do_make_new = false

    sync do
      current_size = @size[0]
      @allocated.keys.each do |t|
        unless t.alive?
          (to_disconnect ||= []) << @allocated.delete(t)
          current_size -= 1
        end
      end
    
      do_make_new = true if can_make_new?(current_size)
    end

    begin
      preallocated_make_new if do_make_new
    ensure
      if to_disconnect
        to_disconnect.each{|conn| disconnect_connection(conn)}
        fill_queue
      end
    end
  end
  
  # Assigns a connection to the supplied thread, if one
  # is available.
  #
  # This should return a connection is one is available within the timeout,
  # or raise PoolTimeout if a connection could not be acquired within the timeout.
  #
  # Calling code should not have the mutex when calling this.
  def acquire(thread)
    if conn = @queue.pop(timeout: 0) || try_make_new || @queue.pop(timeout: @timeout)
      sync{@allocated[thread] = conn}
    else
      name = db.opts[:name]
      raise ::Sequel::PoolTimeout, "timeout: #{@timeout}#{", database name: #{name}" if name}"
    end
  end

  # Returns the connection owned by the supplied thread,
  # if any. The calling code should NOT already have the mutex before calling this.
  def owned_connection(thread)
    sync{@allocated[thread]}
  end
  
  # Create the maximum number of connections immediately. This should not be called
  # with a true argument unless no code is currently operating on the database.
  #
  # Calling code should not have the mutex when calling this.
  def preconnect(concurrent = false)
    if concurrent
      if times = sync{@max_size > (size = @size[0]) ? @max_size - size : false}
        Array.new(times){Thread.new{if conn = try_make_new; @queue.push(conn) end}}.map(&:value)
      end
    else
      while conn = try_make_new
        @queue.push(conn)
      end
    end

    nil
  end

  # Releases the connection assigned to the supplied thread back to the pool.
  #
  # Calling code should not have the mutex when calling this.
  def release(thread)
    checkin_connection(sync{@allocated.delete(thread)})
    nil
  end

  # Adds a connection to the queue of available connections, returns the connection.
  def checkin_connection(conn)
    @queue.push(conn)
    conn
  end

  # Yield to the block while inside the mutex.
  #
  # Calling code should not have the mutex when calling this.
  def sync
    @mutex.synchronize{yield}
  end
end
