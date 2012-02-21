module Sequel
  module Plugins
    # The instance_hooks plugin allows you to add hooks to specific instances,
    # by passing a block to a _hook method (e.g. before_save_hook{do_something}).
    # The block is executed when the hook is called (e.g. before_save).
    #
    # All of the standard hooks are supported, except for after_initialize.
    # Instance level before hooks are executed in reverse order of addition before
    # calling super.  Instance level after hooks are executed in order of addition
    # after calling super.  If any of the instance level before hook blocks return
    # false, no more instance level before hooks are called and false is returned.
    #
    # Instance level hooks for before and after are cleared after all related
    # after level instance hooks have run.  This means that if you add a before_create
    # and before_update instance hooks to a new object, the before_create hook will
    # be run the first time you save the object (creating it), and the before_update
    # hook will be run the second time you save the object (updating it), and no
    # hooks will be run the third time you save the object.
    # 
    # Usage:
    #
    #   # Add the instance hook methods to all model subclass instances (called before loading subclasses)
    #   Sequel::Model.plugin :instance_hooks
    #
    #   # Add the instance hook methods just to Album instances
    #   Album.plugin :instance_hooks
    module InstanceHooks
      module InstanceMethods 
        BEFORE_HOOKS = Sequel::Model::BEFORE_HOOKS
        AFTER_HOOKS = Sequel::Model::AFTER_HOOKS - [:after_initialize]
        HOOKS = BEFORE_HOOKS + AFTER_HOOKS
        HOOKS.each{|h| class_eval("def #{h}_hook(&block); add_instance_hook(:#{h}, &block); self end", __FILE__, __LINE__)}
        
        BEFORE_HOOKS.each{|h| class_eval("def #{h}; run_before_instance_hooks(:#{h}) == false ? false : super end", __FILE__, __LINE__)}
        AFTER_HOOKS.each{|h| class_eval(<<-END, __FILE__, __LINE__ + 1)}
          def #{h}
            super
            run_after_instance_hooks(:#{h})
            @instance_hooks.delete(:#{h})
            @instance_hooks.delete(:#{h.to_s.sub('after', 'before')})
          end
        END
        
        private
        
        # Add the block as an instance level hook.  For before hooks, add it to
        # the beginning of the instance hook's array.  For after hooks, add it
        # to the end.
        def add_instance_hook(hook, &block)
          instance_hooks(hook).send(BEFORE_HOOKS.include?(hook) ? :unshift : :push, block)
        end
        
        # An array of instance level hook blocks for the given hook type.
        def instance_hooks(hook)
          @instance_hooks ||= {}
          @instance_hooks[hook] ||= []
        end
        
        # Run all hook blocks of the given hook type.
        def run_after_instance_hooks(hook)
          instance_hooks(hook).each{|b| b.call}
        end

        # Run all hook blocks of the given hook type.  If a hook block returns false,
        # immediately return false without running the remaining blocks.
        def run_before_instance_hooks(hook)
          instance_hooks(hook).each{|b| return false if b.call == false}
        end
      end
    end
  end
end
