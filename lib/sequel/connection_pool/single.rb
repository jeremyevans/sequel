# This is the fastest connection pool, since it isn't a connection pool at all.
# It is just a wrapper around a single connection that uses the connection pool
# API.
class Sequel::SingleConnectionPool < Sequel::ConnectionPool  
  # The SingleConnectionPool always has a size of 1, since the connection
  # is always available.
  def size
    @conn ? 1 : 0
  end
  
  # Disconnect and immediately reconnect from the database.
  def disconnect(opts=nil, &block)
    block ||= @disconnection_proc
    block.call(@conn) if block
    @conn = nil
  end
  
  # Yield the connection to the block.
  def hold(server=nil)
    begin
      yield(@conn ||= make_new(DEFAULT_SERVER))
    rescue Sequel::DatabaseDisconnectError
      disconnect
      raise
    end
  end

  CONNECTION_POOL_MAP[[true, false]] = self
end
