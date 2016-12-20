# frozen-string-literal: true

module Sequel
  class Model
    # This Module subclass is used by Model.dataset_module
    # to add dataset methods to classes.  It adds a couple
    # of features standard Modules, allowing you to use
    # the same subset method you can call on Model, as well
    # as making sure that public methods added to the module
    # automatically have class methods created for them.
    class DatasetModule < ::Module
      # Store the model related to this dataset module.
      def initialize(model)
        @model = model
      end

      # Define a named filter for this dataset, see
      # Model.subset for details.
      def subset(name, *args, &block)
        @model.subset(name, *args, &block)
      end

      # Alias for subset
      def where(name, *args, &block)
        subset(name, *args, &block)
      end

      %w'exclude exclude_having having'.map(&:to_sym).each do |meth|
        define_method(meth) do |name, *args, &block|
          if block || args.flatten.any?{|arg| arg.is_a?(Proc)}
            @model.def_dataset_method(name){send(meth, *args, &block)}
          else
            key = :"_#{meth}_#{name}_ds"
            @model.def_dataset_method(name) do
              cached_dataset(key){send(meth, *args)}
            end
          end
        end
      end

      meths = (<<-METHS).split.map(&:to_sym)
        distinct grep group group_and_count group_append 
        limit offset order order_append order_prepend 
        select select_all select_append select_group server
      METHS

      meths.each do |meth|
        define_method(meth) do |name, *args, &block|
          if block
            @model.def_dataset_method(name){send(meth, *args, &block)}
          else
            key = :"_#{meth}_#{name}_ds"
            @model.def_dataset_method(name) do
              cached_dataset(key){send(meth, *args)}
            end
          end
        end
      end

      private

      # Add a class method to the related model that
      # calls the dataset method of the same name.
      def method_added(meth)
        @model.send(:def_model_dataset_method, meth)
      end
    end
  end
end
