module Sequel
  # Contains methods that ease metaprogramming, used by some of Sequel's classes.
  module Metaprogramming
    # Add methods to the object's metaclass
    def meta_def(name, &block)
      meta_eval{define_method(name, &block)}
    end 
  
    private
  
    # Make a singleton/class attribute accessor method(s).
    # Replaces the construct:
    #
    #   class << self
    #     attr_accessor *meths
    #   end
    def metaattr_accessor(*meths)
      meta_eval{attr_accessor(*meths)}
    end
  
    # Make a singleton/class attribute reader method(s).
    # Replaces the construct:
    #
    #   class << self
    #     attr_reader *meths
    #   end
    def metaattr_reader(*meths)
      meta_eval{attr_reader(*meths)}
    end
  
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
