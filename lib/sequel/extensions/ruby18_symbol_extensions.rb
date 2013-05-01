# The ruby18_symbol_extensions adds the <, <=, >, >= to Symbol
# to reflect the mathmatical operators.  It also adds the [] method
# to Symbol for creating SQL functions.
#
# Usage of this extension is not recommended. This extension will
# only load on ruby 1.8, so you will not be able to upgrade to
# newer ruby versions if you use it. If you still want to use it,
# you can load it via:
#
#   Sequel.extension :ruby18_symbol_extensions

raise(Sequel::Error, "The ruby18_symbol_extensions is only available on ruby 1.8.") unless RUBY_VERSION < '1.9.0'

class Symbol
  include Sequel::SQL::InequalityMethods

  # Create an SQL Function with the receiver as the function name
  # and the given arguments.
  def [](*args)
    Sequel::SQL::Function.new(self, *args)
  end
end
