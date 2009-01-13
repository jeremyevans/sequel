module Sequel
  class Model
    # Hooks that are safe for public use
    HOOKS = [:after_initialize, :before_create, :after_create, :before_update,
      :after_update, :before_save, :after_save, :before_destroy, :after_destroy,
      :before_validation, :after_validation]

    # Hooks that are only for internal use
    PRIVATE_HOOKS = [:before_update_values, :before_delete]
    
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
        h << [tag, block]
      end
    end

    # Method allows to define own hook method for model. It will define 2
    # methods, ClassMethods#your_hook and InstanceMethods#your_hook. With
    # ClassMethods#your_hook you are able to pass block that should be called
    # when InstanceMethods#your_hook will be invoked.
    # 
    # If InstanceMethods#your_hook returns false method that's invoking your
    # hook should be stoped. It's of course only internal standard, and if you
    # really don't like this behaviour you are not forced to use this. But still
    # it's highly recommended.
    # 
    # Example of usage:
    #
    #  class MyModel
    #   define_hook :before_move_to
    #   before_move_to { true # code that should be executed before move_to method }
    #   def move_to
    #     return if before_move_to
    #     do_something # main move_to method code
    #   end
    #  end
    def self.define_hook(*hooks)
      hooks.each do |hook|
        @hooks[hook] = []
        instance_eval("def #{hook}(method = nil, &block); define_hook_instance_method(:#{hook}); add_hook(:#{hook}, method, &block) end")
        class_eval("def #{hook}; end")
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
    define_hook(*(HOOKS + PRIVATE_HOOKS))
  end
end
