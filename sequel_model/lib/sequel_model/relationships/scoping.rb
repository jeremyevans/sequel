# Authors:
# Mike Ferrier (http://www.mikeferrier.ca)
# Hampton Catlin (http://www.hamptoncatlin.com)

module ScopedStruct
    
  module ClassMethods
    def scope(scope_name, &block)
      MethodCarrier.set_scoped_methods(scope_name, block)
      self.extend MethodCarrier
      self.send(:define_method, scope_name) do
        ProxyObject.new(self, scope_name)
      end
    end
  end
  
  class ProxyObject
    def initialize(parent, scope_name)
      @parent, @scope_name = parent, scope_name
    end
    
    def method_missing(name, *args, &block)
      @parent.send(@scope_name.to_s + "_" + name.to_s, *args, &block)
    end
  end
  
  module MethodCarrier
    def self.extend_object(base)
      @@method_names.each do |method_name|
        base.class_eval %Q(
          alias #{@@scope_name + '_' + method_name} #{method_name}
          undef #{method_name}
        )
      end
    end
    
    def self.set_scoped_methods(scope_name, method_declarations)
      raise SyntaxError.new("No block passed to scope command.") if method_declarations.nil?
      @@scope_name = scope_name.to_s
      @@method_names = extract_method_names(method_declarations).collect{|m| m.to_s}
      raise SyntaxError.new("No methods defined in scope block.") unless @@method_names.any?
      method_declarations.call
    end
    
    def self.extract_method_names(method_declarations)
      cls = BlankSlate.new
      original_methods = cls.methods
      cls.extend(Module.new(&method_declarations))
      cls.methods - original_methods
    end
    
    # Jim Weirich's BlankSlate class from http://onestepback.org/index.cgi/Tech/Ruby/BlankSlate.rdoc
    # We use a slightly modified version of it to figure out what methods were defined in the scope block
    class BlankSlate
      instance_methods.each { |m| undef_method m unless m =~ /^(__|methods|extend)/ }
    end
  end
end

Object.extend(ScopedStruct::ClassMethods)
