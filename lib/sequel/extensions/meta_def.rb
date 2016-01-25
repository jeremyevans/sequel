# frozen-string-literal: true
#
# The meta_def extension is designed for backwards compatibility
# with older Sequel code that uses the meta_def method on
# Database, Dataset, and Model classes and/or instances.  It is
# not recommended for usage in new code.  To load this extension:
#
#   Sequel.extension :meta_def

#
module Sequel
  # Contains meta_def method for adding methods to objects via blocks.
  # Only recommended for backwards compatibility with existing code.
  module Metaprogramming
    # Define a method with the given name and block body on the receiver.
    #
    #   ds = DB[:items]
    #   ds.meta_def(:x){42}
    #   ds.x # => 42
    def meta_def(name, &block)
      (class << self; self end).send(:define_method, name, &block)
    end
  end

  Database.extend Metaprogramming
  Database.send(:include, Metaprogramming)
  Dataset.extend Metaprogramming
  Dataset.send(:include, Metaprogramming)
  if defined?(Model)
    Model.extend Metaprogramming
    Model.send(:include, Metaprogramming)
  end
end
