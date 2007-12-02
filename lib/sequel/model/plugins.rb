module Sequel
  module Plugins; end
  
  class Model
    class << self
      # Loads a plugin for use with the model class
      def is(plugin, *args)
        plugin_module(plugin).apply(self, *args)
      end
      alias_method :is_a, :is
    
      def plugin_module(plugin)
        module_name = plugin.to_s.gsub(/(^|_)(.)/) {$2.upcase}
        if not Sequel::Plugins.const_defined?(module_name)
          require plugin_gem(plugin)
        end
        Sequel::Plugins.const_get(module_name)
      end

      def plugin_gem(plugin)
        "sequel-#{plugin.to_s.gsub('_', '-')}"
      end
    end
  end
end