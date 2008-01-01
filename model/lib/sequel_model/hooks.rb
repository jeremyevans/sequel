module Sequel
  class Model
    HOOKS = [
      :after_initialize,
      :before_create,
      :after_create,
      :before_update,
      :after_update,
      :before_save,
      :after_save,
      :before_destroy,
      :after_destroy
    ]
    
    def self.def_hook_method(m) #:nodoc:
      # write hook def
      hook_def = "
        def self.#{m}(method = nil, &block)
          unless block
            (raise SequelError, 'No hook method specified') unless method
            block = proc {send method}
          end
          add_hook(#{m.inspect}, &block)
        end
      "
      
      instance_eval(hook_def)
    end
    
    HOOKS.each {|h| define_method(h) {}}
    HOOKS.each {|h| def_hook_method(h)}
    
    # Returns the hooks hash for the model class.
    def self.hooks
      @hooks ||= Hash.new {|h, k| h[k] = []}
    end
    
    # Returns true if the model class or any of its ancestors have defined
    # hooks for the given hook key. Notice that this method cannot detect 
    # hooks defined using overridden methods.
    def self.has_hooks?(key)
      has = hooks[key] && !hooks[key].empty?
      has || ((self != Model) && superclass.has_hooks?(key))
    end
    
    def self.add_hook(hook, &block) #:nodoc:
      chain = hooks[hook]
      chain << block
      define_method(hook) do 
        return false if super == false
        chain.each {|h| break false if instance_eval(&h) == false}
      end
    end
  end
end