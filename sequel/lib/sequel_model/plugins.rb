module Sequel
  module Plugins
  end
  
  class Model
    # Loads a plugin for use with the model class, passing optional arguments
    # to the plugin.
    def self.is(plugin, *args)
      m = plugin_module(plugin)
      raise(Error, "Plugin cannot be applied because the model class has no dataset") if m.const_defined?("DatasetMethods") && !@dataset
      if m.respond_to?(:apply)
        m.apply(self, *args)
      end
      if m.const_defined?("InstanceMethods")
        class_def(:"#{plugin}_opts") {args.first}
        include(m::InstanceMethods)
      end
      if m.const_defined?("ClassMethods")
        meta_def(:"#{plugin}_opts") {args.first}
        extend(m::ClassMethods)
      end
      if m.const_defined?("DatasetMethods")
        dataset.meta_def(:"#{plugin}_opts") {args.first}
        dataset.metaclass.send(:include, m::DatasetMethods)
        def_dataset_method *m::DatasetMethods.instance_methods
      end
    end
    metaalias :is_a, :is
  
    ### Private Class Methods ###

    # Returns the gem name for the given plugin.
    def self.plugin_gem(plugin)
      "sequel_#{plugin}"
    end

    # Returns the module for the specified plugin. If the module is not 
    # defined, the corresponding plugin gem is automatically loaded.
    def self.plugin_module(plugin)
      module_name = plugin.to_s.gsub(/(^|_)(.)/) {$2.upcase}
      if not Sequel::Plugins.const_defined?(module_name)
        require plugin_gem(plugin)
      end
      Sequel::Plugins.const_get(module_name)
    end
    metaprivate :plugin_gem, :plugin_module
  end
end
