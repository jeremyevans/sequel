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
  POOL_CLASS_MAP = {
    :threaded => :ThreadedConnectionPool,
    :single => :SingleConnectionPool,
    :sharded_threaded => :ShardedThreadedConnectionPool,
    :sharded_single => :ShardedSingleConnectionPool,
    :timed_queue => :TimedQueueConnectionPool,
    :sharded_timed_queue => :ShardedTimedQueueConnectionPool,
  }
  POOL_CLASS_MAP.to_a.each{|k, v| POOL_CLASS_MAP[k.to_s] = v}
  POOL_CLASS_MAP.freeze

  # Class methods used to return an appropriate pool subclass, separated
  # into a module for easier overridding by extensions.
  module ClassMethods
    # Return a pool subclass instance based on the given options.  If a <tt>:pool_class</tt>
    # option is provided is provided, use that pool class, otherwise
    # use a new instance of an appropriate pool subclass based on the
    # +SEQUEL_DEFAULT_CONNECTION_POOL+ environment variable if set, or
    # the <tt>:single_threaded</tt> and <tt>:servers</tt> options, otherwise.
    def get_pool(db, opts = OPTS)
      connection_pool_class(opts).new(db, opts)
    end
    
    private
    
    # Return a connection pool class based on the given options.
    def connection_pool_class(opts)
      if pc = opts[:pool_class]
        unless pc.is_a?(Class)
          unless name = POOL_CLASS_MAP[pc]
            raise Sequel::Error, "unsupported connection pool type, please pass appropriate class as the :pool_class option"
          end

          require_relative "connection_pool/#{pc}"
          pc = Sequel.const_get(name)
        end

        pc
      elsif pc = ENV['SEQUEL_DEFAULT_CONNECTION_POOL']
        pc = "sharded_#{pc}" if opts[:servers] && !pc.start_with?('sharded_')
        connection_pool_class(:pool_class=>pc)
      else
        pc = if opts[:single_threaded]
          opts[:servers] ? :sharded_single : :single
        elsif RUBY_VERSION >= '3.2'
          opts[:servers] ? :sharded_timed_queue : :timed_queue
        # :nocov:
        else
          opts[:servers] ? :sharded_threaded : :threaded
        end
        # :nocov:

        connection_pool_class(:pool_class=>pc)
      end
    end
  end
  extend ClassMethods

  # The after_connect proc used for this pool.  This is called with each new
  # connection made, and is usually used to set custom per-connection settings.
  # Deprecated.
  attr_reader :after_connect # SEQUEL6: Remove

  # Override the after_connect proc for the connection pool. Deprecated.
  # Disables support for shard-specific :after_connect and :connect_sqls if used.
  def after_connect=(v) # SEQUEL6: Remove
    @use_old_connect_api = true
    @after_connect = v
  end

  # An array of sql strings to execute on each new connection. Deprecated.
  attr_reader :connect_sqls # SEQUEL6: Remove

  # Override the connect_sqls for the connection pool. Deprecated.
  # Disables support for shard-specific :after_connect and :connect_sqls if used.
  def connect_sqls=(v) # SEQUEL6: Remove
    @use_old_connect_api = true
    @connect_sqls = v
  end

  # The Sequel::Database object tied to this connection pool.
  attr_accessor :db

  # Instantiates a connection pool with the given Database and options.
  def initialize(db, opts=OPTS) # SEQUEL6: Remove second argument, always use db.opts
    @db = db
    @use_old_connect_api = false # SEQUEL6: Remove
    @after_connect = opts[:after_connect] # SEQUEL6: Remove
    @connect_sqls = opts[:connect_sqls] # SEQUEL6: Remove
    @error_classes = db.send(:database_error_classes).dup.freeze
  end
  
  # An array of symbols for all shards/servers, which is a single <tt>:default</tt> by default.
  def servers
    [:default]
  end
  
  private

  # Remove the connection from the pool.  For threaded connections, this should be
  # called without the mutex, because the disconnection may block.
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
      if @use_old_connect_api
        # SEQUEL6: Remove block
        conn = @db.connect(server)

        if ac = @after_connect
          if ac.arity == 2
            ac.call(conn, server)
          else
            ac.call(conn)
          end
        end
  
        if cs = @connect_sqls
          cs.each do |sql|
            db.send(:log_connection_execute, conn, sql)
          end
        end

        conn
      else
        @db.new_connection(server)
      end
    rescue Exception=>exception
      raise Sequel.convert_exception_class(exception, Sequel::DatabaseConnectionError)
    end || raise(Sequel::DatabaseConnectionError, "Connection parameters not valid")
  end
end
