# ArrayKeys provide support for accessing array elements by keys. ArrayKeys are
# based on the arrayfields gem by Ara Howard, and can be used as substitutes
# for fetching records tuples as Ruby hashes.
#
# The main advantage offered by ArrayKeys over hashes is that the values are 
# always ordered according to the column order in the query. Another purported
# advantage is that they reduce the memory footprint, but this has turned out
# to be a false claim.
module ArrayKeys
  # The KeySet module contains methods that extend an array of keys to return
  # a key's position in the key set.
  module KeySet
    # Returns the key's position in the key set. Provides indifferent access
    # for symbols and strings.
    def key_pos(key)
      @key_indexes ||= inject({}) {|h, k| h[k.to_sym] = h.size; h}
      @key_indexes[key] || @key_indexes[key.to_sym] || @key_indexes[key.to_s]
    end
 
    # Adds a key to the key set.
    def add_key(key)
      self << key
      @key_indexes[key] = @key_indexes.size
    end
    
    # Removes a key from the key set by its index.
    def del_key(idx)
      delete_at(idx)
      @key_indexes = nil # reset key indexes
    end
  end
  
  # The KeyAccess provides a large part of the Hash API for arrays with keys.
  module KeyAccess
    # Returns a value referenced by an array index or a key.
    def [](idx, *args)
      if String === idx or Symbol === idx
        (idx = @keys.key_pos(idx)) ? super(idx, *args) : nil
      else
        super
      end
    end
 
    # Sets the value referenced by an array index or a key.
    def []=(idx,*args)
      if String === idx or Symbol === idx
        idx = @keys.key_pos(idx) || @keys.add_key(idx.to_sym)
      end
      super(idx, *args)
    end
    
    # Stores a value by index or key.
    def store(k, v); self[k] = v; end
    
    # Slices the array, and returns an array with its keys sliced accordingly.
    def slice(*args)
      s = super(*args)
      s.keys = @keys.slice(*args)
      s
    end
    
    # Converts the array into a hash.
    def to_hash
      h = {}
      each_with_index {|v, i| h[@keys[i].to_sym] = v}
      h
    end
    alias_method :to_h, :to_hash
    
    # Iterates over each key-value pair in the array.
    def each_pair
      each_with_index {|v, i| yield @keys[i], v}
    end
    
    # Iterates over the array's associated keys.
    def each_key(&block)
      @keys.each(&block)
    end
    
    # Iterates over the array's values.
    def each_value(&block)
      each(&block)
    end
    
    # Deletes a value by its key.
    def delete(key, *args)
      if (idx = @keys.key_pos(key))
        delete_at(idx)
      end
    end
    
    # Deletes a value by its index.
    def delete_at(idx)
      super(idx)
      @keys = @keys.clone
      @keys.del_key(idx)
    end
    
    # Returns true if the array's key set contains the given key.
    def include?(k)
      @keys.include?(k) || @keys.include?(k.to_sym) || @keys.include?(k.to_s)
    end
    
    # Returns true if the array's key set contains the given key.
    def has_key?(k)
      @keys.include?(k) || @keys.include?(k.to_sym) || @keys.include?(k.to_s)
    end
    alias_method :member?, :has_key?
    alias_method :key?, :has_key?
    
    # Returns true if the array contains the given value.
    def has_value?(k); orig_include?(k); end
    alias_method :value?, :has_value?
    
    # Fetches a value by its key and optionally passes it through the given
    # block:
    #
    #   row.fetch(:name) {|v| v.to_sym}
    #
    # You can also give a default value
    #
    #   row.fetch(:name, 'untitled')
    #
    def fetch(k, *args, &block)
      if idx = @keys.key_pos(k)
        v = at idx
      else
        !args.empty? ? (v = args.first) : (raise IndexError, "key not found")
      end
      block ? block[v] : v
    end
    
    # Returns self.
    def values
      self
    end
    
    # Creates a copy of self with the same key set.
    def dup
      copy = super
      copy.keys = @keys
      copy
    end
    
    # Creates a copy of self with a copy of the key set.
    def clone
      copy = super
      copy.keys = @keys.clone
      copy
    end
    
    # Returns an array merged from self and the given array.
    def merge(values, &block)
      clone.merge!(values, &block)
    end
    
    # Merges the given array with self, optionally passing the values from self
    # through the given block:
    #
    #   row.merge!(new_values) {|k, old, new| (k == :name) ? old : new}
    #
    def merge!(values, &block)
      values.each_pair do |k, v|
        self[k] = (has_key?(k) && block) ? block[k, self[k], v] : v
      end
      self
    end
    alias_method :update, :merge!
    alias_method :update!, :merge!
  end
  
  # The ArrayExtensions module provides extensions for the Array class.
  module ArrayExtensions
    attr_reader :keys
    
    # Sets the key set for the array. Once a key set has been set for an array,
    # it is extended with the KeyAccess API
    def keys=(keys)
      extend ArrayKeys::KeyAccess if keys
      @keys = keys.frozen? ? keys.dup : keys
      unless @keys.respond_to?(:key_pos)
        @keys.extend(ArrayKeys::KeySet)
      end
    end
 
    alias_method :columns, :keys
    alias_method :columns=, :keys=
  end
  
  # The DatasetExtensions module provides extensions that modify
  # a dataset to return Array tuples instead of Hash tuples.
  module DatasetExtensions
    # Fetches a dataset's records, converting each tuple into an array with 
    # keys.
    def array_tuples_each(opts = nil, &block)
      fetch_rows(select_sql(opts)) {|h| block[Array.from_hash(h)]}
    end

    # Provides the corresponding behavior to Sequel::Dataset#update_each_method,
    # using array tuples.
    def array_tuples_update_each_method
      # warning: ugly code generation ahead
      if @row_proc && @transform
        class << self
          def each(opts = nil, &block)
            if opts && opts[:naked]
              fetch_rows(select_sql(opts)) {|r| block[transform_load(Array.from_hash(r))]}
            else
              fetch_rows(select_sql(opts)) {|r| block[@row_proc[transform_load(Array.from_hash(r))]]}
            end
            self
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
            self
          end
        end
      elsif @transform
        class << self
          def each(opts = nil, &block)
            fetch_rows(select_sql(opts)) {|r| block[transform_load(Array.from_hash(r))]}
            self
          end
        end
      else
        class << self
          def each(opts = nil, &block)
            fetch_rows(select_sql(opts)) {|r| block[Array.from_hash(r)]}
            self
          end
        end
      end
    end
  end
end

# Array extensions.
class Array
  alias_method :orig_include?, :include?

  include ArrayKeys::ArrayExtensions

  # Converts a hash into an array with keys.
  def self.from_hash(h)
    a = []; a.keys = []
    a.merge!(h)
  end
end

module Sequel
  # Modifies all dataset classes to fetch records as arrays with keys. By 
  # default records are fetched as hashes.
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
          alias_method :each, :array_tuples_each
          alias_method :update_each_method, :array_tuples_update_each_method
        end
      end
    end
  end
  
  # Modifies all dataset classes to fetch records as hashes.
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