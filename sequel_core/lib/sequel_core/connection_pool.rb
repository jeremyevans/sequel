require 'thread'

# A ConnectionPool manages access to database connections by keeping
# multiple connections and giving threads exclusive access to each
# connection.
class ConnectionPool
  attr_reader :mutex
  
  # The maximum number of connections.
  attr_reader :max_size
  
  # The proc used to create a new connection.
  attr_accessor :connection_proc
  
  attr_reader :available_connections, :allocated, :created_count

  # Constructs a new pool with a maximum size. If a block is supplied, it
  # is used to create new connections as they are needed.
  #
  #   pool = ConnectionPool.new(10) {MyConnection.new(opts)}
  #
  # The connection creation proc can be changed at any time by assigning a 
  # Proc to pool#connection_proc.
  #
  #   pool = ConnectionPool.new(10)
  #   pool.connection_proc = proc {MyConnection.new(opts)}
  def initialize(opts, &block)
    @max_size = opts[:max_connections]
    @mutex = Mutex.new
    @connection_proc = block

    @available_connections = []
    @allocated = []
    @created_count = 0
    @timeout = opts[:pool_timeout]
    @sleep_time = opts[:pool_sleep_time]
    @reuse_connections = opts[:pool_reuse_connections]
    @convert_exceptions = opts[:pool_convert_exceptions]
  end
  
  # Returns the number of created connections.
  def size
    @created_count
  end
  
  # Assigns a connection to the current thread, yielding the connection
  # to the supplied block.
  # 
  #   pool.hold {|conn| conn.execute('DROP TABLE posts')}
  # 
  # Pool#hold is re-entrant, meaning it can be called recursively in
  # the same thread without blocking.
  #
  # If no connection is available, Pool#hold will block until a connection
  # is available.
  def hold
    begin
      t = Thread.current
      time = Time.new
      timeout = time + @timeout
      sleep_time = @sleep_time
      reuse = @reuse_connections
      if reuse == :always && conn = owned_connection(t)
        return yield(conn)
      end
      reuse = reuse == :allow ? true : false
      until conn = acquire(t)
        if reuse && (conn = owned_connection(t))
          return yield(conn)
        end
        if Time.new > timeout
          if (@reuse_connections == :last_resort) && (conn = owned_connection(t))
            return yield(conn)
          end
          raise(::Sequel::Error::PoolTimeoutError)
        end
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
      @mutex.synchronize do 
        x = @allocated.assoc(thread)
        x[1] if x
      end
    end
    
    # Assigns a connection to the supplied thread, if one is available.
    def acquire(thread)
      @mutex.synchronize do
        if conn = available
          @allocated << [thread, conn]
          conn
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
        x = @allocated.delete([thread, conn])
        @available_connections << x[1] if x
      end
    end
end

# A SingleThreadedPool acts as a replacement for a ConnectionPool for use
# in single-threaded applications. ConnectionPool imposes a substantial
# performance penalty, so SingleThreadedPool is used to gain some speed.
class SingleThreadedPool
  attr_reader :conn
  attr_writer :connection_proc
  
  # Initializes the instance with the supplied block as the connection_proc.
  def initialize(opts, &block)
    @connection_proc = block
    @convert_exceptions = opts[:pool_convert_exceptions]
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
