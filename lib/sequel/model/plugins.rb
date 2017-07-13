# frozen-string-literal: true

module Sequel
  # Empty namespace that plugins should use to store themselves,
  # so they can be loaded via Model.plugin.
  #
  # Plugins should be modules with one of the following conditions:
  # * A singleton method named apply, which takes a model, 
  #   additional arguments, and an optional block.  This is called
  #   the first time the plugin is loaded for this model (unless it was
  #   already loaded by an ancestor class), before including/extending
  #   any modules, with the arguments
  #   and block provided to the call to Model.plugin.
  # * A module inside the plugin module named ClassMethods,
  #   which will extend the model class.
  # * A module inside the plugin module named InstanceMethods,
  #   which will be included in the model class.
  # * A module inside the plugin module named DatasetMethods,
  #   which will extend the model's dataset.
  # * A singleton method named configure, which takes a model, 
  #   additional arguments, and an optional block.  This is called
  #   every time the Model.plugin method is called, after including/extending
  #   any modules.
  module Plugins
    # In the given module +mod+, define methods that are call the same method
    # on the dataset.  This is designed for plugins to define dataset methods
    # inside ClassMethods that call the implementations in DatasetMethods.
    #
    # This should not be called with untrusted input or method names that
    # can't be used literally, since it uses class_eval.
    def self.def_dataset_methods(mod, meths)
      Array(meths).each do |meth|
        mod.class_eval("def #{meth}(*args, &block); dataset.#{meth}(*args, &block) end", __FILE__, __LINE__)
      end
    end

    # Add method to +mod+ that overrides inherited_instance_variables to include the
    # values in this hash.
    def self.inherited_instance_variables(mod, hash)
      mod.send(:define_method, :inherited_instance_variables) do ||
        super().merge!(hash)
      end
    end

    # Add method to +mod+ that overrides set_dataset to call the method afterward.
    def self.after_set_dataset(mod, meth)
      mod.send(:define_method, :set_dataset) do |*a|
        r = super(*a)
        # Allow calling private class methods as methods this specifies are usually private
        send(meth)
        r
      end
    end
  end
end
