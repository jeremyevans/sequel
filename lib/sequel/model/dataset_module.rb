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

      # Alias for where.
      def subset(name, *args, &block)
        where(name, *args, &block)
      end

      %w'where exclude exclude_having having'.map(&:to_sym).each do |meth|
        define_method(meth) do |name, *args, &block|
          if block || args.flatten.any?{|arg| arg.is_a?(Proc)}
            define_method(name){send(meth, *args, &block)}
          else
            key = :"_#{meth}_#{name}_ds"
            define_method(name) do
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

      # Define a method in the module
      def self.def_dataset_caching_method(mod, meth)
        mod.send(:define_method, meth) do |name, *args, &block|
          if block
            define_method(name){send(meth, *args, &block)}
          else
            key = :"_#{meth}_#{name}_ds"
            define_method(name) do
              cached_dataset(key){send(meth, *args)}
            end
          end
        end
      end

      meths.each do |meth|
        def_dataset_caching_method(self, meth)
      end

      private

      # Add a class method to the related model that
      # calls the dataset method of the same name.
      def method_added(meth)
        @model.send(:def_model_dataset_method, meth) if public_method_defined?(meth)
        super
      end
    end

    @dataset_module_class = DatasetModule
  end
end
