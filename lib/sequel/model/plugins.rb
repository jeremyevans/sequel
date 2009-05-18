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
    # Loads a plugin for use with the model class, passing optional arguments
    # to the plugin.  If the plugin is a module, load it directly.  Otherwise,
    # require the plugin from either sequel/plugins/#{plugin} or
    # sequel_#{plugin}, and then attempt to load the module using a
    # the camelized plugin name under Sequel::Plugins.
    def self.plugin(plugin, *args)
      arg = args.first
      block = lambda{arg}
      m = plugin.is_a?(Module) ? plugin : plugin_module(plugin)
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
        if @dataset
          dataset.meta_def(:"#{plugin}_opts", &block)
          dataset.extend(m::DatasetMethods)
        end
        dataset_method_modules << m::DatasetMethods
        meths = m::DatasetMethods.public_instance_methods.reject{|x| NORMAL_METHOD_NAME_REGEXP !~ x.to_s}
        def_dataset_method(*meths) unless meths.empty?
      end
    end
    
    module ClassMethods
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
