module Sequel
  module Plugins; end
  
  class Model
    class << self
      # Loads a plugin for use with the model class, passing optional arguments
      # to the plugin.
      def is(plugin, *args)
        plugin_module(plugin).apply(self, *args)
      end
      alias_method :is_a, :is
    
      # Returns the module for the specified plugin. If the module is not 
      # defined, the corresponding plugin gem is automatically loaded.
      def plugin_module(plugin)
        module_name = plugin.to_s.gsub(/(^|_)(.)/) {$2.upcase}
        if not Sequel::Plugins.const_defined?(module_name)
          require plugin_gem(plugin)
        end
        Sequel::Plugins.const_get(module_name)
      end

      # Returns the gem name for the given plugin.
      def plugin_gem(plugin)
        "sequel_#{plugin}"
      end
    end
  end
end