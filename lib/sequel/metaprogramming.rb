module Sequel
  # Contains meta_def method for adding methods to objects via blocks, used by some of Sequel's classes and objects.
  module Metaprogramming
    # Define a method with the given name and block body on the receiver.
    def meta_def(name, &block)
      (class << self; self end).send(:define_method, name, &block)
    end
  end
end
