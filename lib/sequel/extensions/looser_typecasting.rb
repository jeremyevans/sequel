# The LooserTypecasting extension loosens the default database typecasting
# for the following types:
#
# :float :: use to_f instead of Float()
# :integer :: use to_i instead of Integer()
# :decimal :: don't check string conversion with Float()
# :string :: silently allow hash and array conversion to string
#
# To load the extension into the database:
#
#   DB.extension :looser_typecasting

#
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

    # Typecast the value to an Integer using to_i instead of Kernel.Integer
    def typecast_value_string(value)
      value.to_s
    end

    # Typecast the value to a BigDecimal, without checking if strings
    # have a valid format.
    def typecast_value_decimal(value)
      if value.is_a?(String)
        BigDecimal.new(value)
      else
        super
      end
    end
  end

  Database.register_extension(:looser_typecasting, LooserTypecasting)
end

