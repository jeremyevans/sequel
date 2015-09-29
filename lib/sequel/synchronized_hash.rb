module Sequel
  # A Hash with synchronized access. This class only provides methods actually
  # used by Sequel (e.g. [] and []=).
  class SynchronizedHash
    def initialize
      @mutex = Mutex.new
      @hash  = {}
    end

    # Returns the value of the given key
    def [](key)
      @mutex.synchronize { @hash[key] }
    end

    # Sets the key +key+ to +value+
    def []=(key, value)
      @mutex.synchronize { @hash[key] = value }
    end

    # Deletes the given key
    def delete(key)
      @mutex.synchronize { @hash.delete(key) }
    end

    # Returns the keys of the Hash
    def keys
      @mutex.synchronize { @hash.keys }
    end

    # Returns whether or not the Hash is empty
    def empty?
      @mutex.synchronize { @hash.empty? }
    end
  end
end
