# The base connection pool class, which all other connection pools are based
# on.  This class is not instantiated directly, but subclasses should at
# the very least implement the following API:
#
# initialize(Hash, &block) :: The +block+ is used as the connection proc,
#                             which should accept a single symbol argument.
# hold(Symbol, &block) :: Yield a connection object (obtained from calling
#                         the block passed to +initialize+) to the current block. For sharded
#                         connection pools, the Symbol passed is the shard/server to use.
# disconnect(Symbol, &block) :: Disconnect the connection object.  If a
#                               block is given, pass the connection option to it, otherwise use the
#                               <tt>:disconnection_proc</tt> option in the hash passed to initialize.  For sharded
#                               connection pools, the Symbol passed is the shard/server to use.
# servers :: An array of shard/server symbols for all shards/servers that this
#            connection pool recognizes.
# size :: an integer representing the total number of connections in the pool,
#         or for the given shard/server if sharding is supported.
#
# For sharded connection pools, the sharded API adds the following methods:
#
# add_servers(Array of Symbols) :: start recognizing all shards/servers specified
#                                  by the array of symbols.
# * remove_servers(Array of Symbols) :: no longer recognize all shards/servers
#                                       specified by the array of symbols.
class Sequel::ConnectionPool
  # The default server to use
  DEFAULT_SERVER = :default

  # A map of [single threaded, sharded] values to symbols or ConnectionPool subclasses.
  CONNECTION_POOL_MAP = {[true, false] => :single,
    [true, true] => :sharded_single,
    [false, false] => :threaded,
    [false, true] => :sharded_threaded}

  # Class methods used to return an appropriate pool subclass, separated
  # into a module for easier overridding by extensions.
  module ClassMethods
    # Return a pool subclass instance based on the given options.  If a <tt>:pool_class</tt>
    # option is provided is provided, use that pool class, otherwise
    # use a new instance of an appropriate pool subclass based on the
    # <tt>:single_threaded</tt> and <tt>:servers</tt> options.
    def get_pool(opts = {}, &block)
      case v = connection_pool_class(opts)
      when Class
        v.new(opts, &block)
      when Symbol
        Sequel.ts_require("connection_pool/#{v}")
        connection_pool_class(opts).new(opts, &block) || raise(Sequel::Error, "No connection pool class found")
      end
    end

    private

    # Return a connection pool class based on the given options.
    def connection_pool_class(opts)
      CONNECTION_POOL_MAP[opts[:pool_class]] || opts[:pool_class] || CONNECTION_POOL_MAP[[!!opts[:single_threaded], !!opts[:servers]]]
    end
  end
  extend ClassMethods

  # The after_connect proc used for this pool.  This is called with each new
  # connection made, and is usually used to set custom per-connection settings.
  attr_accessor :after_connect

  # The disconnect_proc used for the pool.  This is called with each connection
  # that is disconnected, usually to clean up related resources.
  attr_accessor :disconnection_proc

  # Instantiates a connection pool with the given options.  The block is called
  # with a single symbol (specifying the server/shard to use) every time a new
  # connection is needed.  The following options are respected for all connection
  # pools:
  # :after_connect :: The proc called after each new connection is made, with the
  #                   connection object, useful for customizations that you want to apply to all
  #                   connections.
  # :disconnection_proc :: The proc called when removing connections from the pool,
  #                        which is passed the connection to disconnect.
  def initialize(opts={}, &block)
    raise(Sequel::Error, "No connection proc specified") unless @connection_proc = block
    @disconnection_proc = opts[:disconnection_proc]
    @after_connect = opts[:after_connect]
  end

  # Alias for +size+, not aliased directly for ease of subclass implementation
  def created_count(*args)
    size(*args)
  end

  # An array of symbols for all shards/servers, which is a single <tt>:default</tt> by default.
  def servers
    [DEFAULT_SERVER]
  end

  private

  # Return a new connection by calling the connection proc with the given server name,
  # and checking for connection errors.
  def make_new(server)
    begin
      conn = @connection_proc.call(server)
      @after_connect.call(conn) if @after_connect
    rescue Exception=>exception
      raise Sequel.convert_exception_class(exception, Sequel::DatabaseConnectionError)
    end
    raise(Sequel::DatabaseConnectionError, "Connection parameters not valid") unless conn
    conn
  end
end
