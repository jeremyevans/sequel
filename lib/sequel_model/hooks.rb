module Sequel
  class Model
    # Hooks that are safe for public use
    HOOKS = [:after_initialize, :before_create, :after_create, :before_update,
      :after_update, :before_save, :after_save, :before_destroy, :after_destroy,
      :before_validation, :after_validation]

    # Hooks that are only for internal use
    PRIVATE_HOOKS = [:before_update_values, :before_delete]
    
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
    # All of Sequel's standard hook types are also implemented using this
    # method.
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
    def self.add_hook_type(*hooks)
      hooks.each do |hook|
        @hooks[hook] = []
        instance_eval("def #{hook}(method = nil, &block); define_hook_instance_method(:#{hook}); add_hook(:#{hook}, method, &block) end")
        class_eval("def #{hook}; end")
      end
    end

    # Returns true if there are any hook blocks for the given hook.
    def self.has_hooks?(hook)
      !@hooks[hook].empty?
    end

    # Yield every block related to the given hook.
    def self.hook_blocks(hook)
      @hooks[hook].each{|k,v| yield v}
    end

    ### Private Class Methods ###

    # Add a hook block to the list of hook methods.
    # If a non-nil tag is given and it already is in the list of hooks,
    # replace it with the new block.
    def self.add_hook(hook, tag, &block) #:nodoc:
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

    # Define a hook instance method that calls the run_hooks instance method.
    def self.define_hook_instance_method(hook) #:nodoc:
      class_eval("def #{hook}; run_hooks(:#{hook}); end")
    end

    private_class_method :add_hook, :define_hook_instance_method

    private

    # Runs all hook blocks of given hook type on this object.
    # Stops running hook blocks and returns false if any hook block returns false.
    def run_hooks(hook)
      model.hook_blocks(hook){|block| return false if instance_eval(&block) == false}
    end
    
    # For performance reasons, we define empty hook instance methods, which are
    # overwritten with real hook instance methods whenever the hook class method is called.
    add_hook_type(*(HOOKS + PRIVATE_HOOKS))
  end
end
