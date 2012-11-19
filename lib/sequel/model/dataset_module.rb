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
        define_method(name){filter(*args, &block)}
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
