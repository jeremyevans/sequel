# frozen-string-literal: true

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
    #
    # Note that returning false in any before hook block will skip further
    # before hooks and abort the action.  So if a before_save hook block returns
    # false, future before_save hook blocks are not called, and the save is aborted.
    # 
    # Usage:
    #
    #   # Allow use of hook class methods in all model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :hook_class_methods
    #
    #   # Allow the use of hook class methods in the Album class
    #   Album.plugin :hook_class_methods
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
          Model::HOOKS.concat(hooks)
          hooks.each do |hook|
            @hooks[hook] = []
            instance_eval("def #{hook}(method = nil, &block); add_hook(:#{hook}, method, &block) end", __FILE__, __LINE__)
            class_eval("def #{hook}; model.hook_blocks(:#{hook}){|b| return false if instance_eval(&b) == false}; end", __FILE__, __LINE__)
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
        (Model::BEFORE_HOOKS - [:before_save, :before_destroy]).each{|h| class_eval("def #{h}; model.hook_blocks(:#{h}){|b| return false if instance_eval(&b) == false}; super; end", __FILE__, __LINE__)}
        (Model::AFTER_HOOKS - [:after_save, :after_destroy, :after_commit, :after_rollback, :after_destroy_commit, :after_destroy_rollback]).each{|h| class_eval("def #{h}; super; model.hook_blocks(:#{h}){|b| instance_eval(&b)}; end", __FILE__, __LINE__)}

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
            return false if instance_eval(&b) == false
          end
          super
          if model.has_hooks?(:after_destroy_rollback)
            db.after_rollback{model.hook_blocks(:after_destroy_rollback){|b| instance_eval(&b)}}
          end
        end

        def before_save
          model.hook_blocks(:before_save) do |b|
            return false if instance_eval(&b) == false
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
