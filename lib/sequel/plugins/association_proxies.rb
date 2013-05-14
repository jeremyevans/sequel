module Sequel
  module Plugins
    # Sequel by default does not use proxies for associations.  The association
    # method for *_to_many associations returns an array, and the association_dataset
    # method returns a dataset.  This plugin makes the association method return a proxy
    # that will load the association and call a method on the association array if sent
    # an array method, and otherwise send the method to the association's dataset.
    # 
    # You can override which methods to forward to the dataset by passing a block to the plugin:
    #
    #   plugin :association_proxies do |meth, args, &block|
    #     [:find, :where, :create].include?(meth)
    #   end
    #
    # If the block returns false or nil, the method is sent to the array of associated
    # objects.  Otherwise, the method is sent to the association dataset.
    # 
    # Usage:
    #
    #   # Use association proxies in all model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :association_proxies
    #
    #   # Use association proxies in a specific model subclass
    #   Album.plugin :association_proxies
    module AssociationProxies
      def self.configure(model, &block)
        model.instance_eval do
          @association_proxy_to_dataset = block if block
          @association_proxy_to_dataset ||= AssociationProxy::DEFAULT_PROXY_TO_DATASET
        end
      end

      # A proxy for the association.  Calling an array method will load the
      # associated objects and call the method on the associated object array.
      # Calling any other method will call that method on the association's dataset.
      class AssociationProxy < BasicObject
        array = []

        # Default proc used to determine whether to sent the method to the dataset.
        # If the array would respond to it, sends it to the array instead of the dataset.
        DEFAULT_PROXY_TO_DATASET = proc{|meth, args, &block| !array.respond_to?(meth)}

        # Set the association reflection to use, and whether the association should be
        # reloaded if an array method is called.
        def initialize(instance, reflection, reload=nil)
          @instance = instance
          @reflection = reflection
          @reload = reload
        end

        # Call the method given on the array of associated objects if the method
        # is an array method, otherwise call the method on the association's dataset.
        def method_missing(meth, *args, &block)
          (@instance.model.association_proxy_to_dataset.call(meth, args, &block) ? @instance.send(@reflection.dataset_method) : @instance.send(:load_associated_objects, @reflection, @reload)).send(meth, *args, &block)
        end
      end

      module ClassMethods
        # Proc that accepts a method name, array of arguments, and block and
        # should return a truthy value to send the method to the dataset instead of the
        # array of associated objects.
        attr_reader :association_proxy_to_dataset

        Plugins.inherited_instance_variables(self, :@association_proxy_to_dataset=>nil)

        # Changes the association method to return a proxy instead of the associated objects
        # directly.
        def def_association_method(opts)
          opts.returns_array? ? association_module_def(opts.association_method, opts){|*r| AssociationProxy.new(self, opts, r[0])} : super
        end
      end
    end
  end
end
