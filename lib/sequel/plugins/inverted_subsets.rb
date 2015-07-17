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
    #   Album.plugin(:inverted_subsets){|name| "exclude_#{name}" }
    #
    # Usage:
    #
    #   # Add inverted subsets in the Album class
    #   Album.plugin :inverted_subsets
    #
    #   # This will now create two methods, published and not_published
    #   Album.subset :published, { published: true }
    #
    #   Album.published.sql
    #   # SELECT * FROM albums WHERE (published IS TRUE)
    #
    #   Album.not_published.sql
    #   # SELECT * FROM albums WHERE (published IS NOT TRUE)
    #
    module InvertedSubsets
      module ClassMethods

        # Define a not_ prefixed subset which inverts the subset condition.
        def subset(name, *args, &block)
          super

          inverted_name = @inverted_subsets_name_block.call(name)
          def_dataset_method(inverted_name){exclude(*args, &block)}
        end
      end

      # Store the supplied block for calling later when subsets are defined, or
      # create a default one if we need to.
      def self.configure(model, &block)
        model.instance_eval do
          unless block_given?
            block = Proc.new{|name| "not_#{name}" }
          end
          @inverted_subsets_name_block = block
        end
      end

    end

  end
end
