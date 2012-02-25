# A ShardedSingleConnectionPool is a single threaded connection pool that
# works with multiple shards/servers.
class Sequel::ShardedSingleConnectionPool < Sequel::ConnectionPool
  # Initializes the instance with the supplied block as the connection_proc.
  #
  # The single threaded pool takes the following options:
  #
  # * :servers - A hash of servers to use.  Keys should be symbols.  If not
  #   present, will use a single :default server.  The server name symbol will
  #   be passed to the connection_proc.
  # * :servers_hash - The base hash to use for the servers.  By default,
  #   Sequel uses Hash.new(:default).  You can use a hash with a default proc
  #   that raises an error if you want to catch all cases where a nonexistent
  #   server is used.
  def initialize(opts={}, &block)
    super
    @conns = {}
    @servers = opts.fetch(:servers_hash, Hash.new(:default))
    add_servers([:default])
    add_servers(opts[:servers].keys) if opts[:servers]
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
      server = pick_server(server)
      yield(@conns[server] ||= make_new(server))
    rescue Sequel::DatabaseDisconnectError
      disconnect_server(server, &@disconnection_proc)
      raise
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
  
  # The number of different shards/servers this pool is connected to.
  def size
    @conns.length
  end
  
  private
  
  # Disconnect from the given server, if connected.
  def disconnect_server(server, &block)
    if conn = @conns.delete(server)
      block.call(conn) if block
    end
  end

  # If the server given is in the hash, return it, otherwise, return the default server.
  def pick_server(server)
    @servers[server]
  end
  
  CONNECTION_POOL_MAP[[true, true]] = self
end
