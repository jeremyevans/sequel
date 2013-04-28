module Sequel
  module Plugins
    # InputTransformer is a plugin that allows generic transformations
    # of input values in model column setters.  Example:
    #
    #   Album.plugin :input_transformer
    #   Album.add_input_transformer(:reverser){|v| v.is_a?(String) ? v.reverse : v}
    #   album = Album.new(:name=>'foo')
    #   album.name # => 'oof'
    #
    # You can specifically set some columns to skip some input
    # input transformers:
    #
    #   Album.skip_input_transformer(:reverser, :foo)
    #   Album.new(:foo=>'bar').foo # => 'bar'
    # 
    # Usage:
    #
    #   # Make all model subclass instances support input transformers (called before loading subclasses)
    #   Sequel::Model.plugin :input_transformer
    #
    #   # Make the Album class support input transformers 
    #   Album.plugin :input_transformer
    module InputTransformer
      def self.apply(model, *)
        model.instance_eval do
          @input_transformers = {}
          @input_transformer_order = []
          @skip_input_transformer_columns = {}
        end
      end

      # If an input transformer is given in the plugin call,
      # add it as a transformer
      def self.configure(model, transformer_name=nil, &block)
        model.add_input_transformer(transformer_name, &block) if transformer_name || block
      end

      module ClassMethods
        # Hash of input transformer name symbols to transformer callables.
        attr_reader :input_transformers

        # The order in which to call the input transformers.
        attr_reader :input_transformer_order

        Plugins.inherited_instance_variables(self, :@skip_input_transformer_columns=>:hash_dup, :@input_transformers=>:dup, :@input_transformer_order=>:dup)

        # Add an input transformer to this model.
        def add_input_transformer(transformer_name, &block)
          raise(Error, 'must provide both transformer name and block when adding input transformer') unless transformer_name && block
          @input_transformers[transformer_name] = block
          @input_transformer_order.unshift(transformer_name)
          @skip_input_transformer_columns[transformer_name] = []
        end

        # Set columns that the transformer should skip.
        def skip_input_transformer(transformer_name, *columns)
          @skip_input_transformer_columns[transformer_name].concat(columns).uniq!
        end

        # Return true if the transformer should not be called for the given column.
        def skip_input_transformer?(transformer_name, column)
          @skip_input_transformer_columns[transformer_name].include?(column)
        end
      end

      module InstanceMethods
        # Transform the input using all of the transformers, except those explicitly
        # skipped, before setting the value in the model object.
        def []=(k, v)
          model.input_transformer_order.each do |transformer_name|
            v = model.input_transformers[transformer_name].call(v) unless model.skip_input_transformer?(transformer_name, k)
          end
          super
        end
      end
    end
  end
end
