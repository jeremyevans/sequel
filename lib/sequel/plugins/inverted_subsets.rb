# frozen-string-literal: true

module Sequel
  module Plugins
    # The inverted_subsets plugin adds another method for each defined
    # subset, which inverts the condition supplied. By default, inverted
    # subset method names are prefixed with not_.
    #
    # You can change the prefix, or indeed entirely customise the inverted names,
    # by passing a block to the plugin configuration:
    #
    #   # Use an exclude_ prefix for inverted subsets instead of not_
    #   Album.plugin(:inverted_subsets){|name| "exclude_#{name}"}
    #
    # Usage:
    #
    #   # Add inverted subsets in the Album class
    #   Album.plugin :inverted_subsets
    #
    #   # This will now create two methods, published and not_published
    #   Album.subset :published, :published => true
    #
    #   Album.published.sql
    #   # SELECT * FROM albums WHERE (published IS TRUE)
    #
    #   Album.not_published.sql
    #   # SELECT * FROM albums WHERE (published IS NOT TRUE)
    #
    module InvertedSubsets
      # Default naming for inverted subsets
      DEFAULT_NAME_BLOCK = lambda{|name| "not_#{name}"}

      # Store the supplied block for calling later when subsets are defined, or
      # create a default one if we need to.
      def self.configure(model, &block)
        model.instance_variable_set(:@inverted_subsets_name_block, block || DEFAULT_NAME_BLOCK)
      end

      module ClassMethods
        Plugins.inherited_instance_variables(self, :@inverted_subsets_name_block => nil)

        # Define a not_ prefixed subset which inverts the subset condition.
        def subset(name, *args, &block)
          super
          def_dataset_method(@inverted_subsets_name_block.call(name)){exclude(*args, &block)}
        end
      end
    end
  end
end
