module Sequel
  class Model
    # Hooks that are safe for public use
    HOOKS = [:after_initialize, :before_create, :after_create, :before_update,
      :after_update, :before_save, :after_save, :before_destroy, :after_destroy,
      :before_validation, :after_validation]

    # Hooks that are only for internal use
    PRIVATE_HOOKS = [:before_update_values, :before_delete]
    
    # Returns true if the model class or any of its ancestors have defined
    # hooks for the given hook key. Notice that this method cannot detect 
    # hooks defined using overridden methods.
    def self.has_hooks?(key)
      has = hooks[key] && !hooks[key].empty?
      has || ((self != Model) && superclass.has_hooks?(key))
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
      h = hooks[hook]
      if tag && (old = h.find{|x| x[0] == tag})
        old[1] = block
      else
        h << [tag, block]
      end
    end

    # Returns all hook methods for the given type of hook for this
    # model class and its ancestors.
    def self.all_hooks(hook) # :nodoc:
      ((self == Model ? [] : superclass.send(:all_hooks, hook)) + hooks[hook].collect{|x| x[1]})
    end
      
    # Returns the hooks hash for this model class.
    def self.hooks #:nodoc:
      @hooks ||= Hash.new {|h, k| h[k] = []}
    end

    # Runs all hooks of type hook on the given object.
    # Returns false if any hook returns false.
    def self.run_hooks(hook, object) #:nodoc:
      all_hooks(hook).each{|b| return false if object.instance_eval(&b) == false}
    end

    private_class_method :add_hook, :all_hooks, :hooks, :run_hooks

    (HOOKS + PRIVATE_HOOKS).each do |hook|
      instance_eval("def #{hook}(method = nil, &block); add_hook(:#{hook}, method, &block) end")
      define_method(hook){model.send(:run_hooks, hook, self)}
    end
  end
end
