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
    
    # Some fancy code generation here in order to define the hook class methods...
    HOOK_METHOD_STR = %Q{
      def self.%s(method = nil, &block)
        unless block
          (raise SequelError, 'No hook method specified') unless method
          block = proc {send method}
        end
        add_hook(%s, &block)
      end
    }
    
    class << self
      # Returns true if the model class or any of its ancestors have defined
      # hooks for the given hook key. Notice that this method cannot detect 
      # hooks defined using overridden methods.
      def has_hooks?(key)
        has = hooks[key] && !hooks[key].empty?
        has || ((self != Model) && superclass.has_hooks?(key))
      end
      private
        def def_hook_method(m) #:nodoc:
          instance_eval(HOOK_METHOD_STR % [m.to_s, m.inspect])
        end
        
        # Returns the hooks hash for the model class.
        def hooks #:nodoc:
          @hooks ||= Hash.new {|h, k| h[k] = []}
        end
        
        def add_hook(hook, &block) #:nodoc:
          chain = hooks[hook]
          chain << block
          define_method(hook) do 
            return false if super == false
            chain.each {|h| break false if instance_eval(&h) == false}
          end
        end
    end

    HOOKS.each do |h|
      define_method(h){}
      def_hook_method(h)
    end
  end
end
