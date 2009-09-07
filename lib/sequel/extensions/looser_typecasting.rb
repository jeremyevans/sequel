# The LooserTypecasting extension changes the float and integer typecasting to
# use the looser .to_f and .to_i instead of the more strict Kernel.Float and
# Kernel.Integer.  To use it, you should extend the database with the
# Sequel::LooserTypecasting module after loading the extension:
#
#   Sequel.extension :looser_typecasting
#   DB.extend(Sequel::LooserTypecasting)

module Sequel
  module LooserTypecasting
    # Typecast the value to a Float using to_f instead of Kernel.Float
    def typecast_value_float(value)
      value.to_f
    end

    # Typecast the value to an Integer using to_i instead of Kernel.Integer
    def typecast_value_integer(value)
      value.to_i
    end
  end
end
