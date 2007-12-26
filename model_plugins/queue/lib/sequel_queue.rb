module Sequel
  module Plugins
    module Queue
      # Apply the plugin to the model.
      def self.apply(model, options = {})
        # Define each class method;
        # model.class_def(:method_name) do |parameter1,parameter2,...|
        #   dataset... # work with the dataset
        # end

        model.send(:include, InstanceMethods)
      end

      module InstanceMethods
        # Define methods that will be instance-specific here.
      end
    end
  end
end
