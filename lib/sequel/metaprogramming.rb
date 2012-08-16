module Sequel
  # Contains meta_def method for adding methods to objects via blocks, used by some of Sequel's classes and objects.
  module Metaprogramming
    # Define a method with the given name and block body on the receiver.
    #
    #   ds = DB[:items]
    #   ds.meta_def(:x){42}
    #   ds.x # => 42
    def meta_def(name, &block)
      (class << self; self end).send(:define_method, name, &block)
    end

    # Remove a meta method with the given name.
    # This is useful in conjunction with meta_def to temporarily change the behavior of an object.
    #
    #   object = Object.new
    #   object.extend(Sequel::Metaprogramming)
    #   object.to_s #=> "#<Object:0x000000022b6dd8>"
    #   
    #   object.meta_def(:to_s){ "my fancy to_s" }
    #   object.to_s #=> "my fancy to_s"
    #   object.meta_remove(:to_s)
    #   
    #   object.to_s #=> "#<Object:0x000000022b6dd8>"
    #
    def meta_remove(name)
      (class << self; self end).send(:remove_method, name)
    end
  end
end
