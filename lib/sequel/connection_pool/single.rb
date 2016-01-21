# frozen-string-literal: true

# This is the fastest connection pool, since it isn't a connection pool at all.
# It is just a wrapper around a single connection that uses the connection pool
# API.
class Sequel::SingleConnectionPool < Sequel::ConnectionPool  
  # Yield the connection if one has been made.
  def all_connections
    yield @conn if @conn
  end

  # Disconnect the connection from the database.
  def disconnect(opts=nil)
    return unless @conn
    db.disconnect_connection(@conn)
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

  # The SingleConnectionPool always has a maximum size of 1.
  def max_size
    1
  end
  
  def pool_type
    :single
  end
  
  # The SingleConnectionPool always has a size of 1 if connected
  # and 0 if not.
  def size
    @conn ? 1 : 0
  end

  private

  # Make sure there is a valid connection.
  def preconnect
    hold{}
  end

  CONNECTION_POOL_MAP[[true, false]] = self
end
