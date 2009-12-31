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
  
  # The mutex that protects access to the other internal variables.  You must use
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
    @max_size = Integer(opts[:max_connections] || 4)
    raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1
    @mutex = Mutex.new
    @connection_proc = block
    @disconnection_proc = opts[:disconnection_proc]    
    @available_connections = {}
    @allocated = {}
    @connections_to_remove = []
    @servers = Hash.new(:default)
    add_servers([:default])
    add_servers(opts[:servers].keys) if opts[:servers]
    @timeout = Integer(opts[:pool_timeout] || 5)
    @sleep_time = Float(opts[:pool_sleep_time] || 0.001)
    @convert_exceptions = opts.include?(:pool_convert_exceptions) ? opts[:pool_convert_exceptions] : true
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
  
  # An array of connections opened but not currently used, for the given
  # server. Nonexistent servers will return nil. Treat this as read only, do
  # not modify the resulting object.
  def available_connections(server=:default)
    @available_connections[server]
  end
  
  # The total number of connections opened for the given server, should
  # be equal to available_connections.length + allocated.length.  Nonexistent
  # servers will return the created count of the default server.
  def created_count(server=:default)
    server = @servers[server]
    @allocated[server].length + @available_connections[server].length
  end
  alias size created_count
  
  # Removes all connection currently available on all servers, optionally
  # yielding each connection to the given block. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used.  If connections are being used, they are scheduled to be
  # disconnected as soon as they are returned to the pool.
  # 
  # Once a connection is requested using #hold, the connection pool
  # creates new connections to the database. Options:
  # * :server - Should be a symbol specifing the server to disconnect from,
  #   or an array of symbols to specify multiple servers.
  def disconnect(opts={}, &block)
    block ||= @disconnection_proc
    sync do
      (opts[:server] ? Array(opts[:server]) : @servers.keys).each do |s|
        disconnect_server(s, &block)
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
    begin
      sync{server = @servers[server]}
      t = Thread.current
      if conn = owned_connection(t, server)
        return yield(conn)
      end
      begin
        unless conn = acquire(t, server)
          time = Time.now
          timeout = time + @timeout
          sleep_time = @sleep_time
          sleep sleep_time
          until conn = acquire(t, server)
            raise(::Sequel::PoolTimeout) if Time.now > timeout
            sleep sleep_time
          end
        end
        yield conn
      rescue Sequel::DatabaseDisconnectError
        sync{@connections_to_remove << conn} if conn
        raise
      ensure
        sync{release(t, conn, server)} if conn
      end
    rescue StandardError 
      raise
    rescue Exception => e
      raise(@convert_exceptions ? RuntimeError.new(e.message) : e)
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
          disconnect_server(server, &@disconnection_proc)
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

  private

  # Assigns a connection to the supplied thread for the given server, if one
  # is available. The calling code should NOT already have the mutex when
  # calling this.
  def acquire(thread, server)
    sync do
      if conn = available(server)
        allocated(server)[thread] = conn
      end
    end
  end
  
  # Returns an available connection to the given server. If no connection is
  # available, tries to create a new connection. The calling code should already
  # have the mutex before calling this.
  def available(server)
    available_connections(server).pop || make_new(server)
  end

  # Disconnect from the given server.  Disconnects available connections
  # immediately, and schedules currently allocated connections for disconnection
  # as soon as they are returned to the pool. The calling code should already
  # have the mutex before calling this.
  def disconnect_server(server, &block)
    if conns = available_connections(server)
      conns.each{|conn| block.call(conn)} if block
      conns.clear
    end
    @connections_to_remove.concat(allocated(server).values)
  end

  # Creates a new connection to the given server if the size of the pool for
  # the server is less than the maximum size of the pool. The calling code
  # should already have the mutex before calling this.
  def make_new(server)
    if (n = created_count(server)) >= @max_size
      allocated(server).to_a.each{|t, c| release(t, c, server) unless t.alive?}
      n = nil
    end
    if (n || created_count(server)) < @max_size
      raise(Sequel::Error, "No connection proc specified") unless @connection_proc
      begin
        conn = @connection_proc.call(server)
      rescue Exception=>exception
        raise Sequel.convert_exception_class(exception, Sequel::DatabaseConnectionError)
      end
      raise(Sequel::DatabaseConnectionError, "Connection parameters not valid") unless conn
      conn
    end
  end
  
  # Returns the connection owned by the supplied thread for the given server,
  # if any. The calling code should NOT already have the mutex before calling this.
  def owned_connection(thread, server)
    sync{@allocated[server][thread]}
  end
  
  # Releases the connection assigned to the supplied thread and server. If the
  # server or connection given is scheduled for disconnection, remove the
  # connection instead of releasing it back to the pool.
  # The calling code should already have the mutex before calling this.
  def release(thread, conn, server)
    if @connections_to_remove.include?(conn)
      remove(thread, conn, server)
    else
      available_connections(server) << allocated(server).delete(thread)
    end
  end

  # Removes the currently allocated connection from the connection pool. The
  # calling code should already have the mutex before calling this.
  def remove(thread, conn, server)
    @connections_to_remove.delete(conn)
    allocated(server).delete(thread) if @servers.include?(server)
    @disconnection_proc.call(conn) if @disconnection_proc
  end
  
  # Yield to the block while inside the mutex. The calling code should NOT
  # already have the mutex before calling this.
  def sync
    @mutex.synchronize{yield}
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
    @servers = Hash.new(:default)
    add_servers([:default])
    add_servers(opts[:servers].keys) if opts[:servers]
    @convert_exceptions = opts.include?(:pool_convert_exceptions) ? opts[:pool_convert_exceptions] : true
  end
  
  # Adds new servers to the connection pool. Primarily used in conjunction with master/slave
  # or shard configurations. Allows for dynamic expansion of the potential slaves/shards
  # at runtime. servers argument should be an array of symbols. 
  def add_servers(servers)
    servers.each{|s| @servers[s] = s}
  end
  
  # The connection for the given server.
  def conn(server=:default)
    @conns[@servers[server]]
  end
  
  # Disconnects from the database. Once a connection is requested using
  # #hold, the connection is reestablished. Options:
  # * :server - Should be a symbol specifing the server to disconnect from,
  #   or an array of symbols to specify multiple servers.
  def disconnect(opts={}, &block)
    block ||= @disconnection_proc
    (opts[:server] ? Array(opts[:server]) : servers).each{|s| disconnect_server(s, &block)}
  end
  
  # Yields the connection to the supplied block for the given server.
  # This method simulates the ConnectionPool#hold API.
  def hold(server=:default)
    begin
      begin
        server = @servers[server]
        yield(c = (@conns[server] ||= make_new(server)))
      rescue Sequel::DatabaseDisconnectError
        disconnect_server(server, &@disconnection_proc)
        raise
      end
    rescue Exception => e
      # if the error is not a StandardError it is converted into RuntimeError.
      raise(@convert_exceptions && !e.is_a?(StandardError) ? RuntimeError.new(e.message) : e)
    end
  end
  
  # Remove servers from the connection pool. Primarily used in conjunction with master/slave
  # or shard configurations.  Similar to disconnecting from all given servers,
  # except that after it is used, future requests for the server will use the
  # :default server instead.
  def remove_servers(servers)
    raise(Sequel::Error, "cannot remove default server") if servers.include?(:default)
    servers.each do |server|
      disconnect_server(server, &@disconnection_proc)
      @servers.delete(server)
    end
  end
  
  # Return an array of symbols for servers in the connection pool.
  def servers
    @servers.keys
  end
  
  private
  
  # Disconnect from the given server, if connected.
  def disconnect_server(server, &block)
    if conn = @conns.delete(server)
      block.call(conn) if block
    end
  end
  
  # Return a connection to the given server, raising DatabaseConnectionError
  # if the connection_proc raises an error or doesn't return a valid connection.
  def make_new(server)
    begin
      conn = @connection_proc.call(server)
    rescue Exception=>exception
      raise Sequel.convert_exception_class(exception, Sequel::DatabaseConnectionError)
    end
    raise(Sequel::DatabaseConnectionError, "Connection parameters not valid") unless conn
    conn
  end
end
