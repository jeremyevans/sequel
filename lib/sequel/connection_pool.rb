# frozen-string-literal: true

# The base connection pool class, which all other connection pools are based
# on.  This class is not instantiated directly, but subclasses should at
# the very least implement the following API:
#
# initialize(Database, Hash) :: Initialize using the passed Sequel::Database
#                               object and options hash.
# hold(Symbol, &block) :: Yield a connection object (obtained from calling
#                         the block passed to +initialize+) to the current block. For sharded
#                         connection pools, the Symbol passed is the shard/server to use.
# disconnect(Symbol) :: Disconnect the connection object.  For sharded
#                       connection pools, the Symbol passed is the shard/server to use.
# servers :: An array of shard/server symbols for all shards/servers that this
#            connection pool recognizes.
# size :: an integer representing the total number of connections in the pool,
#         or for the given shard/server if sharding is supported.
# max_size :: an integer representing the maximum size of the connection pool,
#             or the maximum size per shard/server if sharding is supported.
#
# For sharded connection pools, the sharded API adds the following methods:
#
# add_servers(Array of Symbols) :: start recognizing all shards/servers specified
#                                  by the array of symbols.
# remove_servers(Array of Symbols) :: no longer recognize all shards/servers
#                                     specified by the array of symbols.
class Sequel::ConnectionPool
  OPTS = Sequel::OPTS

  # The default server to use
  DEFAULT_SERVER = :default
  Sequel::Deprecation.deprecate_constant(self, :DEFAULT_SERVER)
  
  # A map of [single threaded, sharded] values to symbols or ConnectionPool subclasses.
  CONNECTION_POOL_MAP = {[true, false] => :single, 
    [true, true] => :sharded_single,
    [false, false] => :threaded,
    [false, true] => :sharded_threaded}
  CONNECTION_POOL__MAP = CONNECTION_POOL_MAP
  Sequel::Deprecation.deprecate_constant(self, :CONNECTION_POOL_MAP)
  
  # Class methods used to return an appropriate pool subclass, separated
  # into a module for easier overridding by extensions.
  module ClassMethods
    # Return a pool subclass instance based on the given options.  If a <tt>:pool_class</tt>
    # option is provided is provided, use that pool class, otherwise
    # use a new instance of an appropriate pool subclass based on the
    # <tt>:single_threaded</tt> and <tt>:servers</tt> options.
    def get_pool(db, opts = OPTS)
      case v = connection_pool_class(opts)
      when Class
        v.new(db, opts)
      when Symbol
        require("sequel/connection_pool/#{v}")
        connection_pool_class(opts).new(db, opts) || raise(Sequel::Error, "No connection pool class found")
      end
    end
    
    private
    
    # Return a connection pool class based on the given options.
    def connection_pool_class(opts)
      if opts[:pool_class] && !opts[:pool_class].is_a?(Class) && ![:threaded, :single, :sharded_threaded, :sharded_single].include?(opts[:pool_class])
        Sequel::Deprecation.deprecate("Using an unrecognized :pool_class option", "Use a class for the :pool_class option to select a custom pool class, or one of the following symbols for one of the default pool classes: :threaded, :single, :sharded_threaded, :sharded_single")
      end
      CONNECTION_POOL__MAP[opts[:pool_class]] || opts[:pool_class] || CONNECTION_POOL__MAP[[!!opts[:single_threaded], !!opts[:servers]]]
    end
  end
  extend ClassMethods

  # The after_connect proc used for this pool.  This is called with each new
  # connection made, and is usually used to set custom per-connection settings.
  attr_accessor :after_connect

  # The Sequel::Database object tied to this connection pool.
  attr_accessor :db

  # Instantiates a connection pool with the given options.  The block is called
  # with a single symbol (specifying the server/shard to use) every time a new
  # connection is needed.  The following options are respected for all connection
  # pools:
  # :after_connect :: A callable object called after each new connection is made, with the
  #                   connection object (and server argument if the callable accepts 2 arguments),
  #                   useful for customizations that you want to apply to all connections.
  # :preconnect :: Automatically create the maximum number of connections, so that they don't
  #                need to be created as needed.  This is useful when connecting takes a long time
  #                and you want to avoid possible latency during runtime.
  #                Set to :concurrently to create the connections in separate threads. Otherwise
  #                they'll be created sequentially.
  def initialize(db, opts=OPTS)
    @db = db
    @after_connect = opts[:after_connect]
    @error_classes = db.send(:database_error_classes).dup.freeze
  end
  
  # Alias for +size+, not aliased directly for ease of subclass implementation
  def created_count(*args)
    Sequel::Deprecation.deprecate("Sequel::ConnectionPool#created_count", "Use #size instead")
    size(*args)
  end
  
  # An array of symbols for all shards/servers, which is a single <tt>:default</tt> by default.
  def servers
    [:default]
  end
  
  private

  # Remove the connection from the pool.
  def disconnect_connection(conn)
    db.disconnect_connection(conn)
  end

  # Whether the given exception is a disconnect exception.
  def disconnect_error?(exception)
    exception.is_a?(Sequel::DatabaseDisconnectError) || db.send(:disconnect_error?, exception, OPTS)
  end
  
  # Return a new connection by calling the connection proc with the given server name,
  # and checking for connection errors.
  def make_new(server)
    begin
      conn = @db.connect(server)
      if ac = @after_connect
        if ac.arity == 2
          ac.call(conn, server)
        else
          ac.call(conn)
        end
      end
    rescue Exception=>exception
      raise Sequel.convert_exception_class(exception, Sequel::DatabaseConnectionError)
    end
    raise(Sequel::DatabaseConnectionError, "Connection parameters not valid") unless conn
    conn
  end
end
