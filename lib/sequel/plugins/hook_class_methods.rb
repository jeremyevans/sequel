module Sequel
  module Plugins
    # Sequel's built-in hook class methods plugin is designed for backwards
    # compatibility.  Its use is not encouraged, it is recommended to use
    # instance methods and super instead of this plugin.  What this plugin
    # allows you to do is, for example:
    #
    #   # Block only, can cause duplicate hooks if code is reloaded
    #   before_save{self.created_at = Time.now}
    #   # Block with tag, safe for reloading
    #   before_save(:set_created_at){self.created_at = Time.now}
    #   # Tag only, safe for reloading, calls instance method
    #   before_save(:set_created_at)
    #
    # Pretty much anything you can do with a hook class method, you can also
    # do with an instance method instead:
    #
    #    def before_save
    #      return false if super == false
    #      self.created_at = Time.now
    #    end
    module HookClassMethods
      # Set up the hooks instance variable in the model.
      def self.apply(model)
        hooks = model.instance_variable_set(:@hooks, {})
        Model::HOOKS.each{|h| hooks[h] = []}
      end

      module ClassMethods
        Model::HOOKS.each{|h| class_eval("def #{h}(method = nil, &block); add_hook(:#{h}, method, &block) end", __FILE__, __LINE__)}

        # This adds a new hook type. It will define both a class
        # method that you can use to add hooks, as well as an instance method
        # that you can use to call all hooks of that type.  The class method
        # can be called with a symbol or a block or both.  If a block is given and
        # and symbol is not, it adds the hook block to the hook type.  If a block
        # and symbol are both given, it replaces the hook block associated with
        # that symbol for a given hook type, or adds it if there is no hook block
        # with that symbol for that hook type.  If no block is given, it assumes
        # the symbol specifies an instance method to call and adds it to the hook
        # type.
        #
        # If any hook block returns false, the instance method will return false
        # immediately without running the rest of the hooks of that type.
        #
        # It is recommended that you always provide a symbol to this method,
        # for descriptive purposes.  It's only necessary to do so when you 
        # are using a system that reloads code.
        # 
        # Example of usage:
        #
        #  class MyModel
        #   define_hook :before_move_to
        #   before_move_to(:check_move_allowed){|o| o.allow_move?}
        #   def move_to(there)
        #     return if before_move_to == false
        #     # move MyModel object to there
        #   end
        #  end
        def add_hook_type(*hooks)
          Model::HOOKS.concat(hooks)
          hooks.each do |hook|
            @hooks[hook] = []
            instance_eval("def #{hook}(method = nil, &block); add_hook(:#{hook}, method, &block) end", __FILE__, __LINE__)
            class_eval("def #{hook}; run_hooks(:#{hook}); end", __FILE__, __LINE__)
          end
        end
    
        # Returns true if there are any hook blocks for the given hook.
        def has_hooks?(hook)
          !@hooks[hook].empty?
        end
    
        # Yield every block related to the given hook.
        def hook_blocks(hook)
          @hooks[hook].each{|k,v| yield v}
        end

        # Make a copy of the current class's hooks for the subclass.
        def inherited(subclass)
          super
          hooks = subclass.instance_variable_set(:@hooks, {}) 
          instance_variable_get(:@hooks).each{|k,v| hooks[k] = v.dup}
        end
    
        private
    
        # Add a hook block to the list of hook methods.
        # If a non-nil tag is given and it already is in the list of hooks,
        # replace it with the new block.
        def add_hook(hook, tag, &block)
          unless block
            (raise Error, 'No hook method specified') unless tag
            block = proc {send tag}
          end
          h = @hooks[hook]
          if tag && (old = h.find{|x| x[0] == tag})
            old[1] = block
          else
            if hook.to_s =~ /^before/
              h.unshift([tag,block])
            else
              h << [tag, block]
            end
          end
        end
      end

      module InstanceMethods
        Model::HOOKS.each{|h| class_eval("def #{h}; run_hooks(:#{h}); end", __FILE__, __LINE__)}

        private

        # Runs all hook blocks of given hook type on this object.
        # Stops running hook blocks and returns false if any hook block returns false.
        def run_hooks(hook)
          model.hook_blocks(hook){|block| return false if instance_eval(&block) == false}
        end
      end
    end
  end
end
