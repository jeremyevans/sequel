module Sequel
  class Dataset
    # ---------------------
    # :section: Mutation methods
    # These methods modify the receiving dataset and should be used with care.
    # ---------------------
    
    # All methods that should have a ! method added that modifies
    # the receiver.
    MUTATION_METHODS = %w'add_graph_aliases and cross_join distinct except exclude
    filter for_update from from_self full_join full_outer_join graph
    group group_and_count group_by having inner_join intersect invert join join_table left_join
    left_outer_join limit lock_style naked natural_full_join natural_join
    natural_left_join natural_right_join or order order_by order_more paginate qualify query
    reverse reverse_order right_join right_outer_join select select_all select_append select_more server
    set_defaults set_graph_aliases set_overrides unfiltered ungraphed ungrouped union
    unlimited unordered where with with_recursive with_sql'.collect{|x| x.to_sym}
    
    # Setup mutation (e.g. filter!) methods.  These operate the same as the
    # non-! methods, but replace the options of the current dataset with the
    # options of the resulting dataset.
    def self.def_mutation_method(*meths)
      meths.each do |meth|
        class_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end", __FILE__, __LINE__)
      end
    end
    
    # Add the mutation methods via metaprogramming
    def_mutation_method(*MUTATION_METHODS)
    
    
    # Set the method to call on identifiers going into the database for this dataset
    attr_accessor :identifier_input_method
    
    # Set the method to call on identifiers coming the database for this dataset
    attr_accessor :identifier_output_method

    # Whether to quote identifiers for this dataset
    attr_writer :quote_identifiers
    
    # The row_proc for this database, should be a Proc that takes
    # a single hash argument and returns the object you want
    # each to return.
    attr_accessor :row_proc
    
    # Add a mutation method to this dataset instance.
    def def_mutation_method(*meths)
      meths.each do |meth|
        instance_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end", __FILE__, __LINE__)
      end
    end
    
    private
    
    # Modify the receiver with the results of sending the meth, args, and block
    # to the receiver and merging the options of the resulting dataset into
    # the receiver's options.
    def mutation_method(meth, *args, &block)
      copy = send(meth, *args, &block)
      @opts.merge!(copy.opts)
      self
    end
  end
end