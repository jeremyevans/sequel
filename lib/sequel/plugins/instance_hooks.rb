# frozen-string-literal: true

module Sequel
  module Plugins
    # The instance_hooks plugin allows you to add hooks to specific instances,
    # by passing a block to a _hook method (e.g. before_save_hook{do_something}).
    # The block is executed when the hook is called (e.g. before_save).
    #
    # All of the standard hooks are supported.
    # Instance level before hooks are executed in reverse order of addition before
    # calling super.  Instance level after hooks are executed in order of addition
    # after calling super.
    #
    # Instance level hooks for before and after are cleared after all related
    # after level instance hooks have run.  This means that if you add a before_create
    # and before_update instance hooks to a new object, the before_create hook will
    # be run the first time you save the object (creating it), and the before_update
    # hook will be run the second time you save the object (updating it), and no
    # hooks will be run the third time you save the object.
    #
    # Validation hooks are not cleared until after a successful save.
    # 
    # Usage:
    #
    #   # Add the instance hook methods to all model subclass instances (called before loading subclasses)
    #   Sequel::Model.plugin :instance_hooks
    #
    #   # Add the instance hook methods just to Album instances
    #   Album.plugin :instance_hooks
    module InstanceHooks
      BEFORE_HOOKS, AFTER_HOOKS = Sequel::Model::HOOKS.partition{|l| l.to_s.start_with?('before')}
      Sequel::Deprecation.deprecate_constant(self, :BEFORE_HOOKS)
      Sequel::Deprecation.deprecate_constant(self, :AFTER_HOOKS)
      HOOKS = Sequel::Model::HOOKS
      Sequel::Deprecation.deprecate_constant(self, :HOOKS)
      
      # SEQUEL5: Remove
      DEPRECATION_REPLACEMENTS = {
        :after_commit=>"Use obj.after_save_hook{obj.db.after_commit{}} instead",
        :after_destroy_commit=>"Use obj.after_destroy_hook{obj.db.after_commit{}} instead",
        :after_destroy_rollback=>"Use obj.before_destroy_hook{obj.db.after_rollback{}} instead",
        :after_rollback=>"Use obj.before_save_hook{obj.db.after_rollback{}} instead"
      }.freeze

      module InstanceMethods 
        Sequel::Model::HOOKS.each{|h| class_eval(<<-END , __FILE__, __LINE__+1)}
          def #{h}_hook(&block)
            #{"Sequel::Deprecation.deprecate('Sequel::Model##{h}_hook in the instance_hooks plugin', #{DEPRECATION_REPLACEMENTS[h].inspect})" if DEPRECATION_REPLACEMENTS[h]}
            raise Sequel::Error, "can't add hooks to frozen object" if frozen?
            add_instance_hook(:#{h}, &block)
            self
          end
        END
        
        [:before_create, :before_update, :before_validation].each{|h| class_eval("def #{h}; (@instance_hooks && run_before_instance_hooks(:#{h}) == false) ? false : super end", __FILE__, __LINE__)}
        [:after_create, :after_update].each{|h| class_eval(<<-END, __FILE__, __LINE__ + 1)}
          def #{h}
            super
            return unless @instance_hooks
            run_after_instance_hooks(:#{h})
            @instance_hooks.delete(:#{h})
            @instance_hooks.delete(:#{h.to_s.sub('after', 'before')})
          end
        END

        # Run after destroy instance hooks.
        def after_destroy
          super
          return unless @instance_hooks
          # SEQUEL5: Remove commit/rollback
          if ad = @instance_hooks[:after_destroy_commit]
            db.after_commit{ad.each(&:call)}
          end
          run_after_instance_hooks(:after_destroy)
          @instance_hooks.delete(:after_destroy)
          @instance_hooks.delete(:before_destroy)
          @instance_hooks.delete(:after_destroy_commit)
          @instance_hooks.delete(:after_destroy_rollback)
        end

        # Run after validation instance hooks.
        def after_validation
          super
          return unless @instance_hooks
          run_after_instance_hooks(:after_validation)
        end
        
        # Run after save instance hooks.
        def after_save
          super
          return unless @instance_hooks
          # SEQUEL5: Remove commit/rollback
          if (ac = @instance_hooks[:after_commit])
            db.after_commit{ac.each(&:call)}
          end
          run_after_instance_hooks(:after_save)
          @instance_hooks.delete(:after_save)
          @instance_hooks.delete(:before_save)
          @instance_hooks.delete(:after_validation)
          @instance_hooks.delete(:before_validation)
          @instance_hooks.delete(:after_commit)
          @instance_hooks.delete(:after_rollback)
        end

        # Run before_destroy instance hooks.
        def before_destroy
          return super unless @instance_hooks
          # SEQUEL5: Remove commit/rollback
          if adr = @instance_hooks[:after_destroy_rollback]
            db.after_rollback{adr.each(&:call)}
          end
          # SEQUEL5: No false checking
          run_before_instance_hooks(:before_destroy) == false ? false : super
        end

        # Run before_save instance hooks.
        def before_save
          return super unless @instance_hooks
          # SEQUEL5: Remove commit/rollback
          if ar = @instance_hooks[:after_rollback]
            db.after_rollback{ar.each(&:call)}
          end
          # SEQUEL5: No false checking
          run_before_instance_hooks(:before_save) == false ? false : super
        end
        
        private
        
        # Add the block as an instance level hook.  For before hooks, add it to
        # the beginning of the instance hook's array.  For after hooks, add it
        # to the end.
        def add_instance_hook(hook, &block)
          instance_hooks(hook).send(hook.to_s.start_with?('before') ? :unshift : :push, block)
        end
        
        # An array of instance level hook blocks for the given hook type.
        def instance_hooks(hook)
          @instance_hooks ||= {}
          @instance_hooks[hook] ||= []
        end
        
        # Run all hook blocks of the given hook type.
        def run_after_instance_hooks(hook)
          instance_hooks(hook).each(&:call)
        end

        # Run all hook blocks of the given hook type.  If a hook block returns false,
        # immediately return false without running the remaining blocks.
        def run_before_instance_hooks(hook)
          instance_hooks(hook).each do |b|
            if b.call == false
              Sequel::Deprecation.deprecate("Having #{hook} instance hook block return false to stop evaluation of further #{hook} instance hook blocks", "Instead, call cancel_action inside #{hook} instance hook block")
              return false
            end
          end
        end
      end
    end
  end
end
