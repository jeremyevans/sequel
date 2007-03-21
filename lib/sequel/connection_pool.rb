require 'thread'

module Sequel
  class ConnectionPool
    attr_reader :max_size, :mutex
    attr_accessor :connection_proc
    attr_reader :available_connections, :allocated, :created_count
  
    def initialize(max_size = 4, &block)
      @max_size = max_size
      @mutex = Mutex.new
      @connection_proc = block

      @available_connections = []
      @allocated = {}
      @created_count = 0
    end
    
    def size
      @created_count
    end
    
    def hold
      t = Thread.current
      if (conn = owned_connection(t))
        return yield(conn)
      end
      while !(conn = acquire(t))
        sleep 0.001
      end
      begin
        yield conn
      ensure
        release(t)
      end
    end
    
    def owned_connection(thread)
      @mutex.synchronize {@allocated[thread]}
    end
    
    def acquire(thread)
      @mutex.synchronize do
        @allocated[thread] = available
      end
    end
    
    def available
      @available_connections.pop || make_new
    end
    
    def make_new
      if @created_count < @max_size
        @created_count += 1
        @connection_proc.call
      end
    end
    
    def release(thread)
      @mutex.synchronize do
        @available_connections << @allocated[thread]
        @allocated.delete(thread)
      end
    end
  end
end
