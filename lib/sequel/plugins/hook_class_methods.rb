module Sequel
  module Plugins
    module HookClassMethods
      module ClassMethods
        Model::HOOKS.each{|h| class_eval("def #{h}(method = nil, &block); add_hook(:#{h}, method, &block) end", __FILE__, __LINE__)}

        def add_hook_type(*hooks)
          hooks.each do |hook|
            @hooks[hook] = []
            instance_eval("def #{hook}(method = nil, &block); add_hook(:#{hook}, method, &block) end", __FILE__, __LINE__)
            class_eval("def #{hook}; run_hooks(:#{hook}); end", __FILE__, __LINE__)
          end
        end
      end
    end
  end
end
