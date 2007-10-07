module Sequel
  class Model
    def self.get_hooks(key)
      @hooks ||= {}
      @hooks[key] ||= []
    end
    
    def self.has_hooks?(key)
      !get_hooks(key).empty?
    end
    
    def run_hooks(key)
      self.class.get_hooks(key).each {|h| instance_eval(&h)}
    end
    
    def self.before_save(&block)
      get_hooks(:before_save).unshift(block)
    end
    
    def self.before_create(&block)
      get_hooks(:before_create).unshift(block)
    end
    
    def self.before_update(&block)
      get_hooks(:before_update).unshift(block)
    end
    
    def self.before_destroy(&block)
      get_hooks(:before_destroy).unshift(block)
    end
    
    def self.after_save(&block)
      get_hooks(:after_save) << block
    end
    
    def self.after_create(&block)
      get_hooks(:after_create) << block
    end
    
    def self.after_update(&block)
      get_hooks(:after_update) << block
    end
    
    def self.after_destroy(&block)
      get_hooks(:after_destroy) << block
    end
  end
end