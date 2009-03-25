# A ConnectionPool manages access to database connections by keeping
# multiple connections and giving threads exclusive access to each
# connection.
class Sequel::ConnectionPool
  # The proc used to create a new database connection.
  attr_accessor :connection_proc

  # The proc used to disconnect a database connection.
  attr_accessor :disconnection_proc

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
  # * :disconnection_proc - The proc called when removing connections from the pool.
  # * :max_connections - The maximum number of connections the connection pool
  #   will open (default 4)
  # * :pool_convert_exceptions - Whether to convert non-StandardError based exceptions
  #   to RuntimeError exceptions (default true)
  # * :pool_sleep_time - The amount of time to sleep before attempting to acquire
  #   a connection again (default 0.001)
  # * :pool_timeout - The amount of seconds to wait to acquire a connection
  #   before raising a PoolTimeoutError (default 5)
  # * :servers - A hash of servers to use.  Keys should be symbols.  If not
  #   present, will use a single :default server.  The server name symbol will
  #   be passed to the connection_proc.
  def initialize(opts = {}, &block)
    @max_size = opts[:max_connections] || 4
    @mutex = Mutex.new
    @connection_proc = block
    @disconnection_proc = opts[:disconnection_proc]
    @servers = [:default]
    @servers += opts[:servers].keys - @servers if opts[:servers] 
    @available_connections = Hash.new{|h,k| h[:default]}
    @allocated = Hash.new{|h,k| h[:default]}
    @created_count = Hash.new{|h,k| h[:default]}
    @servers.each do |s|
      @available_connections[s] = []
      @allocated[s] = {}
      @created_count[s] = 0
    end
    @timeout = opts[:pool_timeout] || 5
    @sleep_time = opts[:pool_sleep_time] || 0.001
    @convert_exceptions = opts.include?(:pool_convert_exceptions) ? opts[:pool_convert_exceptions] : true
  end
  
  # A hash of connections currently being used for the given server, key is the
  # Thread, value is the connection.
  def allocated(server=:default)
    @allocated[server]
  end
  
  # An array of connections opened but not currently used, for the given
  # server.
  def available_connections(server=:default)
    @available_connections[server]
  end
  
  # The total number of connections opened for the given server, should
  # be equal to available_connections.length + allocated.length
  def created_count(server=:default)
    @created_count[server]
  end
  alias size created_count
  
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
    begin
      t = Thread.current
      time = Time.new
      timeout = time + @timeout
      sleep_time = @sleep_time
      if conn = owned_connection(t, server)
        return yield(conn)
      end
      until conn = acquire(t, server)
        raise(::Sequel::PoolTimeout) if Time.new > timeout
        sleep sleep_time
      end
      begin
        yield conn
      rescue Sequel::DatabaseDisconnectError => dde
        remove(t, conn, server)
        raise
      ensure
        release(t, conn, server) unless dde
      end
    rescue Exception => e
      raise(@convert_exceptions && !e.is_a?(StandardError) ? RuntimeError.new(e.message) : e)
    end
  end
  
  # Removes all connection currently available on all servers, optionally
  # yielding each connection to the given block. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used. Once a connection is requested using #hold, the connection pool
  # creates new connections to the database.
  def disconnect(&block)
    block ||= @disconnection_proc
    @mutex.synchronize do
      @available_connections.each do |server, conns|
        conns.each{|c| block.call(c)} if block
        conns.clear
        set_created_count(server, allocated(server).length)
      end
    end
  end
  
  private

  # Assigns a connection to the supplied thread for the given server, if one
  # is available.
  def acquire(thread, server)
    @mutex.synchronize do
      if conn = available(server)
        allocated(server)[thread] = conn
      end
    end
  end
  
  # Returns an available connection to the given server. If no connection is
  # available, tries to create a new connection.
  def available(server)
    available_connections(server).pop || make_new(server)
  end
  
  # Creates a new connection to the given server if the size of the pool for
  # the server is less than the maximum size of the pool.
  def make_new(server)
    if @created_count[server] < @max_size
      raise(Sequel::Error, "No connection proc specified") unless @connection_proc
      begin
        conn = @connection_proc.call(server)
      rescue Exception=>exception
        e = Sequel::DatabaseConnectionError.new("#{exception.class} #{exception.message}")
        e.set_backtrace(exception.backtrace)
        raise e
      end
      raise(Sequel::DatabaseConnectionError, "Connection parameters not valid") unless conn
      set_created_count(server, @created_count[server] + 1)
      conn
    end
  end
  
  # Returns the connection owned by the supplied thread for the given server,
  # if any.
  def owned_connection(thread, server)
    @mutex.synchronize{@allocated[server][thread]}
  end
  
  # Releases the connection assigned to the supplied thread and server.
  def release(thread, conn, server)
    @mutex.synchronize do
      allocated(server).delete(thread)
      available_connections(server) << conn
    end
  end

  # Removes the currently allocated connection from the connection pool.
  def remove(thread, conn, server)
    @mutex.synchronize do
      allocated(server).delete(thread)
      set_created_count(server, @created_count[server] - 1)
      @disconnection_proc.call(conn) if @disconnection_proc
    end
  end

  # Set the created count for the given server type
  def set_created_count(server, value)
    server = :default unless @created_count.include?(server)
    @created_count[server] = value
  end
end

# A SingleThreadedPool acts as a replacement for a ConnectionPool for use
# in single-threaded applications. ConnectionPool imposes a substantial
# performance penalty, so SingleThreadedPool is used to gain some speed.
class Sequel::SingleThreadedPool
  # The proc used to create a new database connection
  attr_writer :connection_proc
  
  # The proc used to disconnect a database connection.
  attr_accessor :disconnection_proc

  # Initializes the instance with the supplied block as the connection_proc.
  #
  # The single threaded pool takes the following options:
  #
  # * :disconnection_proc - The proc called when removing connections from the pool.
  # * :pool_convert_exceptions - Whether to convert non-StandardError based exceptions
  #   to RuntimeError exceptions (default true)
  # * :servers - A hash of servers to use.  Keys should be symbols.  If not
  #   present, will use a single :default server.  The server name symbol will
  #   be passed to the connection_proc.
  def initialize(opts={}, &block)
    @connection_proc = block
    @disconnection_proc = opts[:disconnection_proc]
    @conns = {}
    @convert_exceptions = opts.include?(:pool_convert_exceptions) ? opts[:pool_convert_exceptions] : true
  end
  
  # The connection for the given server.
  def conn(server=:default)
    @conns[server]
  end
  
  # Yields the connection to the supplied block for the given server.
  # This method simulates the ConnectionPool#hold API.
  def hold(server=:default)
    begin
      begin
        yield(c = (@conns[server] ||= @connection_proc.call(server)))
      rescue Sequel::DatabaseDisconnectError => dde
        @conns.delete(server)
        @disconnection_proc.call(c) if @disconnection_proc
        raise
      end
    rescue Exception => e
      # if the error is not a StandardError it is converted into RuntimeError.
      raise(@convert_exceptions && !e.is_a?(StandardError) ? RuntimeError.new(e.message) : e)
    end
  end
  
  # Disconnects from the database. Once a connection is requested using
  # #hold, the connection is reestablished.
  def disconnect(&block)
    block ||= @disconnection_proc
    @conns.values.each{|conn| block.call(conn) if block}
    @conns = {}
  end
end
