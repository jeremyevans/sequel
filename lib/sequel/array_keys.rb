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
        (idx = @keys.key_pos(idx)) ? super(idx, *args) : nil
      else
        super
      end
    end
 
    def []=(idx,*args)
      if String === idx or Symbol === idx
        idx = @keys.key_pos(idx) || @keys.add_key(idx.to_sym)
      end
      super(idx, *args)
    end
    
    def store(k, v); self[k] = v; end
    
    def slice(*args)
      s = super(*args)
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
      super(idx)
      @keys = @keys.clone
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
  
  module DatasetExtensions
    def array_tuple_each(opts = nil, &block)
      fetch_rows(select_sql(opts)) {|h| block[Array.from_hash(h)]}
    end

    def array_tuple_update_each_method
      # warning: ugly code generation ahead
      if @row_proc && @transform
        class << self
          def each(opts = nil, &block)
            if opts && opts[:naked]
              fetch_rows(select_sql(opts)) {|r| block[transform_load(Array.from_hash(r))]}
            else
              fetch_rows(select_sql(opts)) {|r| block[@row_proc[transform_load(Array.from_hash(r))]]}
            end
          end
        end
      elsif @row_proc
        class << self
          def each(opts = nil, &block)
            if opts && opts[:naked]
              fetch_rows(select_sql(opts)) {|r| block[Array.from_hash(r)]}
            else
              fetch_rows(select_sql(opts)) {|r| block[@row_proc[Array.from_hash(r)]]}
            end
          end
        end
      elsif @transform
        class << self
          def each(opts = nil, &block)
            fetch_rows(select_sql(opts)) {|r| block[transform_load(Array.from_hash(r))]}
          end
        end
      else
        class << self
          def each(opts = nil, &block)
            fetch_rows(select_sql(opts)) {|r| block[Array.from_hash(r)]}
          end
        end
      end
    end
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

module Sequel
  def self.use_array_tuples
    Dataset.dataset_classes.each do |c|
      c.class_eval do
        if method_defined?(:array_tuples_fetch_rows)
          alias_method :hash_tuples_fetch_rows, :fetch_rows
          alias_method :fetch_rows, :array_tuples_fetch_rows
        else
          alias_method :orig_each, :each
          alias_method :orig_update_each_method, :update_each_method
          include ArrayKeys::DatasetExtensions
          alias_method :each, :array_tuple_each
          alias_method :update_each_method, :array_tuple_update_each_method
        end
      end
    end
  end
  
  def self.use_hash_tuples
    Dataset.dataset_classes.each do |c|
      c.class_eval do
        if method_defined?(:hash_tuples_fetch_rows)
          alias_method :fetch_rows, :hash_tuples_fetch_rows
        else
          if method_defined?(:orig_each)
            alias_method :each, :orig_each
            undef_method :orig_each
          end
          if method_defined?(:orig_update_each_method)
            alias_method :update_each_method, :orig_update_each_method
            undef_method :orig_update_each_method
          end
        end
      end
    end
  end
end