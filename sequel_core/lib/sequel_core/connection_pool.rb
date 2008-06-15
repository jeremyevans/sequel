# A ConnectionPool manages access to database connections by keeping
# multiple connections and giving threads exclusive access to each
# connection.
class Sequel::ConnectionPool
  # A hash of connections currently being used, key is the Thread,
  # value is the connection.
  attr_reader :allocated
  
  # An array of connections opened but not currently used
  attr_reader :available_connections
  
  # The proc used to create a new database connection.
  attr_accessor :connection_proc
  
  # The total number of connections opened, should
  # be equal to available_connections.length +
  # allocated.length
  attr_reader :created_count
  alias_method :size, :created_count

  # The maximum number of connections.
  attr_reader :max_size
  
  # The mutex that protects access to the other internal vairables.  You must use
  # this if you want to manipulate the variables safely.
  attr_reader :mutex
  

  # Constructs a new pool with a maximum size. If a block is supplied, it
  # is used to create new connections as they are needed.
  #
  #   pool = ConnectionPool.new(:max_connections=>10) {MyConnection.new(opts)}
  #
  # The connection creation proc can be changed at any time by assigning a 
  # Proc to pool#connection_proc.
  #
  #   pool = ConnectionPool.new(:max_connections=>10)
  #   pool.connection_proc = proc {MyConnection.new(opts)}
  #
  # The connection pool takes the following options:
  #
  # * :max_connections - The maximum number of connections the connection pool
  #   will open (default 4)
  # * :pool_convert_exceptions - Whether to convert non-StandardError based exceptions
  #   to RuntimeError exceptions (default true)
  # * :pool_sleep_time - The amount of time to sleep before attempting to acquire
  #   a connection again (default 0.001)
  # * :pool_timeout - The amount of seconds to wait to acquire a connection
  #   before raising a PoolTimeoutError (default 5)
  def initialize(opts = {}, &block)
    @max_size = opts[:max_connections] || 4
    @mutex = Mutex.new
    @connection_proc = block

    @available_connections = []
    @allocated = {}
    @created_count = 0
    @timeout = opts[:pool_timeout] || 5
    @sleep_time = opts[:pool_sleep_time] || 0.001
    @convert_exceptions = opts.include?(:pool_convert_exceptions) ? opts[:pool_convert_exceptions] : true
  end
  
  # Chooses the first available connection, or if none are available,
  # creates a new connection.  Passes the connection to the supplied block:
  # 
  #   pool.hold {|conn| conn.execute('DROP TABLE posts')}
  # 
  # Pool#hold is re-entrant, meaning it can be called recursively in
  # the same thread without blocking.
  #
  # If no connection is immediately available and the pool is already using the maximum
  # number of connections, Pool#hold will block until a connection
  # is available or the timeout expires.  If the timeout expires before a
  # connection can be acquired, a Sequel::Error::PoolTimeoutError is 
  # raised.
  def hold
    begin
      t = Thread.current
      time = Time.new
      timeout = time + @timeout
      sleep_time = @sleep_time
      if conn = owned_connection(t)
        return yield(conn)
      end
      until conn = acquire(t)
        raise(::Sequel::Error::PoolTimeoutError) if Time.new > timeout
        sleep sleep_time
      end
      begin
        yield conn
      ensure
        release(t, conn)
      end
    rescue Exception => e
      raise(@convert_exceptions && !e.is_a?(StandardError) ? RuntimeError.new(e.message) : e)
    end
  end
  
  # Removes all connection currently available, optionally yielding each 
  # connection to the given block. This method has the effect of 
  # disconnecting from the database. Once a connection is requested using
  # #hold, the connection pool creates new connections to the database.
  def disconnect(&block)
    @mutex.synchronize do
      @available_connections.each {|c| block[c]} if block
      @available_connections = []
      @created_count = @allocated.size
    end
  end
  
  private

  # Returns the connection owned by the supplied thread, if any.
  def owned_connection(thread)
    @mutex.synchronize{@allocated[thread]}
  end
  
  # Assigns a connection to the supplied thread, if one is available.
  def acquire(thread)
    @mutex.synchronize do
      if conn = available
        @allocated[thread] = conn
      end
    end
  end
  
  # Returns an available connection. If no connection is available,
  # tries to create a new connection.
  def available
    @available_connections.pop || make_new
  end
  
  # Creates a new connection if the size of the pool is less than the
  # maximum size.
  def make_new
    if @created_count < @max_size
      @created_count += 1
      @connection_proc ? @connection_proc.call : \
        (raise Error, "No connection proc specified")
    end
  end
  
  # Releases the connection assigned to the supplied thread.
  def release(thread, conn)
    @mutex.synchronize do
      @allocated.delete(thread)
      @available_connections << conn
    end
  end
end

# A SingleThreadedPool acts as a replacement for a ConnectionPool for use
# in single-threaded applications. ConnectionPool imposes a substantial
# performance penalty, so SingleThreadedPool is used to gain some speed.
#
# Note that using a single threaded pool with some adapters can cause
# errors in certain cases, see Sequel.single_threaded=.
class Sequel::SingleThreadedPool
  # The single database connection for the pool
  attr_reader :conn

  # The proc used to create a new database connection
  attr_writer :connection_proc
  
  # Initializes the instance with the supplied block as the connection_proc.
  #
  # The single threaded pool takes the following options:
  #
  # * :pool_convert_exceptions - Whether to convert non-StandardError based exceptions
  #   to RuntimeError exceptions (default true)
  def initialize(opts={}, &block)
    @connection_proc = block
    @convert_exceptions = opts.include?(:pool_convert_exceptions) ? opts[:pool_convert_exceptions] : true
  end
  
  # Yields the connection to the supplied block. This method simulates the
  # ConnectionPool#hold API.
  def hold
    begin
      @conn ||= @connection_proc.call
      yield @conn
    rescue Exception => e
      # if the error is not a StandardError it is converted into RuntimeError.
      raise(@convert_exceptions && !e.is_a?(StandardError) ? RuntimeError.new(e.message) : e)
    end
  end
  
  # Disconnects from the database. Once a connection is requested using
  # #hold, the connection is reestablished.
  def disconnect(&block)
    block[@conn] if block && @conn
    @conn = nil
  end
end
