# frozen-string-literal: true

module Sequel
  module Plugins
    # Sequel's built-in hook_class_methods plugin is designed for backwards
    # compatibility.  Its use is not encouraged, it is recommended to use
    # instance methods and super instead of this plugin.  This plugin allows
    # calling class methods with blocks to define hooks:
    #
    #   # Block only, can cause duplicate hooks if code is reloaded
    #   before_save{self.created_at = Time.now}
    #   # Block with tag, safe for reloading
    #   before_save(:set_created_at){self.created_at = Time.now}
    #   # Tag only, safe for reloading, calls instance method
    #   before_save(:set_created_at)
    #
    # Pretty much anything you can do with a hook class method, you can also
    # do with an instance method instead (making sure to call super), which is
    # the recommended way to add hooks in Sequel:
    #
    #    def before_save
    #      super
    #      self.created_at = Time.now
    #    end
    #
    # Usage:
    #
    #   # Allow use of hook class methods in all model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :hook_class_methods
    #
    #   # Allow the use of hook class methods in the Album class
    #   Album.plugin :hook_class_methods
    module HookClassMethods
      # SEQUEL5: Remove
      DEPRECATION_REPLACEMENTS = {
        :after_commit=>"Use after_save{db.after_commit{}} instead",
        :after_destroy_commit=>"Use after_destroy{db.after_commit{}} instead",
        :after_destroy_rollback=>"Use before_destroy{db.after_rollback{}} instead",
        :after_rollback=>"Use before_save{db.after_rollback{}} instead"
      }.freeze

      # Set up the hooks instance variable in the model.
      def self.apply(model)
        hooks = model.instance_variable_set(:@hooks, {})
        Model::HOOKS.each{|h| hooks[h] = []}
      end

      module ClassMethods
        Model::HOOKS.each do |h|
          class_eval(<<-END, __FILE__, __LINE__ + 1)
            def #{h}(method = nil, &block)
              #{"Sequel::Deprecation.deprecate('Sequel::Model.#{h} in the hook_class_methods plugin', #{DEPRECATION_REPLACEMENTS[h].inspect})" if DEPRECATION_REPLACEMENTS[h]}
              add_hook(:#{h}, method, &block)
            end
          END
        end

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
        # If any before hook block returns false, the instance method will return false
        # immediately without running the rest of the hooks of that type.
        #
        # It is recommended that you always provide a symbol to this method,
        # for descriptive purposes.  It's only necessary to do so when you 
        # are using a system that reloads code.
        # 
        # Example of usage:
        #
        #  class MyModel
        #   add_hook_type :before_move_to
        #   before_move_to(:check_move_allowed, &:allow_move?)
        #   def move_to(there)
        #     return if before_move_to == false
        #     # move MyModel object to there
        #   end
        #  end
        #
        # Do not call this method with untrusted input, as that can result in
        # arbitrary code execution.
        def add_hook_type(*hooks)
          Sequel::Deprecation.deprecate("Sequel::Model.add_hook_type", "You should add your own hook types manually")
          hooks.each do |hook|
            @hooks[hook] = []
            instance_eval("def #{hook}(method = nil, &block); add_hook(:#{hook}, method, &block) end", __FILE__, __LINE__)
            class_eval("def #{hook}; model.hook_blocks(:#{hook}){|b| return false if instance_eval(&b) == false} end", __FILE__, __LINE__)
          end
        end

        # Freeze hooks when freezing model class.
        def freeze
          @hooks.freeze.each_value(&:freeze)
          super
        end
    
        # Returns true if there are any hook blocks for the given hook.
        def has_hooks?(hook)
          !@hooks[hook].empty?
        end
    
        # Yield every block related to the given hook.
        def hook_blocks(hook)
          @hooks[hook].each{|k,v| yield v}
        end

        Plugins.inherited_instance_variables(self, :@hooks=>:hash_dup)
    
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
        # SEQUEL5: Make :before_save, :before_destroy, :after_save, :after_destroy hooks use metaprogramming instead of specific definitions
        [:before_create, :before_update, :before_validation].each do |h|
          class_eval(<<-END, __FILE__, __LINE__+1)
            def #{h}
              model.hook_blocks(:#{h}) do |b|
                if instance_eval(&b) == false
                  Sequel::Deprecation.deprecate("Having #{h} hook block return false to stop evaluation of further #{h} hook blocks", "Instead, call cancel_action inside #{h} hook block")
                  return false
                end
              end
              super
            end
          END
        end
        [:after_create, :after_update, :after_validation].each{|h| class_eval("def #{h}; super; model.hook_blocks(:#{h}){|b| instance_eval(&b)}; end", __FILE__, __LINE__)}

        def after_destroy
          super
          model.hook_blocks(:after_destroy){|b| instance_eval(&b)}
          if model.has_hooks?(:after_destroy_commit)
            db.after_commit{model.hook_blocks(:after_destroy_commit){|b| instance_eval(&b)}}
          end
        end

        def after_save
          super
          model.hook_blocks(:after_save){|b| instance_eval(&b)}
          if model.has_hooks?(:after_commit)
            db.after_commit{model.hook_blocks(:after_commit){|b| instance_eval(&b)}}
          end
        end

        def before_destroy
          model.hook_blocks(:before_destroy) do |b|
            if instance_eval(&b) == false
              Sequel::Deprecation.deprecate("Having before_destory hook block return false to stop evaluation of further before_destroy hook blocks", "Instead, call cancel_action inside before_destroy hook block")
              return false
            end
          end
          super
          if model.has_hooks?(:after_destroy_rollback)
            db.after_rollback{model.hook_blocks(:after_destroy_rollback){|b| instance_eval(&b)}}
          end
        end

        def before_save
          model.hook_blocks(:before_save) do |b|
            if instance_eval(&b) == false
              Sequel::Deprecation.deprecate("Having before_save hook block return false to stop evaluation of further before_save hook blocks", "Instead, call cancel_action inside before_save hook block")
              return false
            end
          end
          super
          if model.has_hooks?(:after_rollback)
            db.after_rollback{model.hook_blocks(:after_rollback){|b| instance_eval(&b)}}
          end
        end
      end
    end
  end
end
