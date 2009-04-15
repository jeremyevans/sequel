module Sequel
  # Contains methods that ease metaprogramming, used by some of Sequel's classes.
  module Metaprogramming
    # Add methods to the object's metaclass
    def meta_def(name, &block)
      meta_eval{define_method(name, &block)}
    end 
  
    private
  
    # Evaluate the block in the context of the object's metaclass
    def meta_eval(&block)
      metaclass.instance_eval(&block)
    end 
  
    # The hidden singleton lurks behind everyone
    def metaclass
      class << self
        self
      end 
    end 
  end
end
