module Sequel
  class Dataset
    # ---------------------
    # :section: 7 - Mutation methods
    # These methods modify the receiving dataset and should be used with care.
    # ---------------------

    # All methods that should have a ! method added that modifies the receiver.
    MUTATION_METHODS = QUERY_METHODS - [:paginate, :naked]

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
    attr_writer :identifier_input_method

    # Set the method to call on identifiers coming the database for this dataset
    attr_writer :identifier_output_method

    # Whether to quote identifiers for this dataset
    attr_writer :quote_identifiers

    # The row_proc for this database, should be any object that responds to +call+ with
    # a single hash argument and returns the object you want #each to return.
    attr_accessor :row_proc

    # Add a mutation method to this dataset instance.
    def def_mutation_method(*meths)
      meths.each do |meth|
        instance_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end", __FILE__, __LINE__)
      end
    end

    # Remove the row_proc from the current dataset.
    def naked!
      self.row_proc = nil
      self
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
