# Based on the arrayfields gem by Ara Howard

module ArrayKeys
  module KeySet
    def key_pos(key)
      @key_indexes ||= inject({}) {|h, k| h[k.to_sym] = h.size; h}
      @key_indexes[key] || @key_indexes[key.to_sym] || @key_indexes[key.to_s]
    end
 
    def add_key(key)
      self << key
      @key_indexes[key] = @key_indexes.size
    end
    
    def del_key(idx)
      delete_at(idx)
      @key_indexes = nil # reset key indexes
    end
  end
  
  module KeyAccess
    def [](idx, *args)
      if String === idx or Symbol === idx
        (idx = @keys.key_pos(idx)) ? super : nil
      else
        super
      end
    end
 
    def []=(idx,*args)
      if String === idx or Symbol === idx
        idx = @keys.key_pos(idx) || @keys.add_key(idx.to_sym)
      end
      super
    end
    
    def store(k, v); self[k] = v; end
    
    def slice(*args)
      s = super
      s.keys = @keys.slice(*args)
      s
    end
    
    def to_hash
      h = {}
      each_with_index {|v, i| h[@keys[i].to_sym] = v}
      h
    end
    alias_method :to_h, :to_hash
    
    def each_pair
      each_with_index {|v, i| yield @keys[i], v}
    end
    
    def each_key(&block)
      @keys.each(&block)
    end
    
    def each_value(&block)
      each(&block)
    end
    
    def delete(key, *args)
      if (idx = @keys.key_pos(key))
        delete_at(idx)
      end
    end
    
    def delete_at(idx)
      super
      @keys.del_key(idx)
    end
    
    def include?(k)
      @keys.include?(k) || @keys.include?(k.to_sym) || @keys.include?(k.to_s)
    end
    
    def has_key?(k)
      @keys.include?(k) || @keys.include?(k.to_sym) || @keys.include?(k.to_s)
    end
    alias_method :member?, :has_key?
    alias_method :key?, :has_key?
    
    def has_value?(k); orig_include?(k); end
    alias_method :value?, :has_value?
    
    def fetch(k, *args, &block)
      if idx = @keys.key_pos(k)
        v = at idx
      else
        !args.empty? ? (v = args.first) : (raise IndexError, "key not found")
      end
      block ? block[v] : v
    end
    
    def values
      self
    end
    
    def dup
      copy = super
      copy.keys = @keys
      copy
    end
    
    def clone
      copy = super
      copy.keys = @keys.clone
      copy
    end
    
    def merge(values, &block)
      clone.merge!(values, &block)
    end
    
    def merge!(values, &block)
      values.each_pair do |k, v|
        self[k] = (has_key?(k) && block) ? block[k, self[k], v] : v
      end
      self
    end
    alias_method :update, :merge!
    alias_method :update!, :merge!
  end
  
  module ArrayExtensions
    attr_reader :keys
    def keys=(keys)
      extend ArrayKeys::KeyAccess if keys
      @keys = keys.frozen? ? keys.dup : keys
      unless @keys.respond_to?(:key_pos)
        @keys.extend(ArrayKeys::KeySet)
      end
    end
 
    alias_method :fields, :keys
    alias_method :fields=, :keys=
  end
end

class Array
  alias_method :orig_include?, :include?

  include ArrayKeys::ArrayExtensions

  def self.from_hash(h)
    a = []; a.keys = []
    a.merge!(h)
  end
end