module Sequel
  module Plugins
    # Sequel by default does not use proxies for associations.  The association
    # method for *_to_many associations returns an array, and the association_dataset
    # method returns a dataset.  This plugin makes the association method return a proxy
    # that will load the association and call a method on the association array if sent
    # an array method, and otherwise send the method to the association's dataset.
    # 
    # Usage:
    #
    #   # Use association proxies in all model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :association_proxies
    #
    #   # Use association proxies in a specific model subclass
    #   Album.plugin :association_proxies
    module AssociationProxies
      # A proxy for the association.  Calling an array method will load the
      # associated objects and call the method on the associated object array.
      # Calling any other method will call that method on the association's dataset.
      class AssociationProxy < BasicObject
        # Empty array used to check if an array responds to the given method.
        ARRAY = []

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
          (ARRAY.respond_to?(meth) ? @instance.send(:load_associated_objects, @reflection, @reload) : @instance.send(@reflection.dataset_method)).
            send(meth, *args, &block)
        end
      end

      module ClassMethods
        # Changes the association method to return a proxy instead of the associated objects
        # directly.
        def def_association_method(opts)
          opts.returns_array? ? association_module_def(opts.association_method){|*r| AssociationProxy.new(self, opts, r[0])} : super
        end
      end
    end
  end
end
