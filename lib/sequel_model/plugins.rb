module Sequel
  # Empty namespace that plugins should use to store themselves,
  # so they can be loaded via Model.plugin.
  #
  # Plugins should be modules with one of the following conditions:
  # * A singleton method named apply, which takes a model and 
  #   additional arguments.
  # * A module inside the plugin module named InstanceMethods,
  #   which will be included in the model class.
  # * A module inside the plugin module named ClassMethods,
  #   which will extend the model class.
  # * A module inside the plugin module named DatasetMethods,
  #   which will extend the model's dataset.
  module Plugins
  end
  
  class Model
    module ClassMethods
      # Loads a plugin for use with the model class, passing optional arguments
      # to the plugin. If the plugin has a DatasetMethods module and the model
      # doesn't have a dataset, raise an Error.
      def plugin(plugin, *args)
        arg = args.first
        block = lambda{arg}
        m = plugin_module(plugin)
        raise(Error, "Plugin cannot be applied because the model class has no dataset") if m.const_defined?("DatasetMethods") && !@dataset
        if m.respond_to?(:apply)
          m.apply(self, *args)
        end
        if m.const_defined?("InstanceMethods")
          define_method(:"#{plugin}_opts", &block)
          include(m::InstanceMethods)
        end
        if m.const_defined?("ClassMethods")
          meta_def(:"#{plugin}_opts", &block)
          extend(m::ClassMethods)
        end
        if m.const_defined?("DatasetMethods")
          dataset.meta_def(:"#{plugin}_opts", &block)
          dataset.extend(m::DatasetMethods)
          def_dataset_method(*m::DatasetMethods.public_instance_methods)
        end
      end
    
      private
  
      # Returns the new style location for the plugin name.
      def plugin_gem_location(plugin)
        "sequel/plugins/#{plugin}"
      end

      # Returns the old style location for the plugin name.
      def plugin_gem_location_old(plugin)
        "sequel_#{plugin}"
      end
  
      # Returns the module for the specified plugin. If the module is not 
      # defined, the corresponding plugin gem is automatically loaded.
      def plugin_module(plugin)
        module_name = plugin.to_s.gsub(/(^|_)(.)/){|x| x[-1..-1].upcase}
        if not Sequel::Plugins.const_defined?(module_name)
          begin
            require plugin_gem_location(plugin)
          rescue LoadError
            require plugin_gem_location_old(plugin)
          end
        end
        Sequel::Plugins.const_get(module_name)
      end
    end
  end
end
