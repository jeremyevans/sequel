# frozen-string-literal: true
#
# The no_auto_literal_strings extension removes Sequel's automatic conversion
# of strings to literal strings in the dataset filter methods.  By default,
# Sequel considers a string passed to a filter method as a literal string:
#
#   DB[:table].where("name > 'A'")
#
# This is fine, except when the string is derived from user input:
#
#   DB[:table].where("name > '#{params[:user]}'") # SQL Injection!
#
# Sequel does support using placeholders for such strings:
#
#   DB[:table].where("name > ?", params[:user].to_s) # Safe
#
# However, if you forget to user placeholders, and pass a string to a filter
# method that is derived from user input, you open yourself up to SQL injection.
# With this extension, using a plain string in a filter method will result
# in an exception being raised.  You either need to explicitly use a literal
# string:
#
#   DB[:table].where(Sequel.lit("name > ?", params[:user].to_s))
#
# or you need to construct the same SQL using a non-string based approach:
#
#   DB[:table].where{|o| o.name > params[:user].to_s}
#
# Note that as listed in Sequel's security guide, a large number of dataset
# methods call down to a filtering method, and this protects all of those
# cases.
#
# This extension also protects the use of a plain string passed to Dataset#update:
#
#   DB[:table].update("column = column + 1")
#
# Again, you either need to explicitly use a literal string:
#
#   DB[:table].update(Sequel.lit("column = column + 1"))
#
# or construct the same SQL using a non-string based approach:
#
#   DB[:table].update(:column => Sequel[:column] + 1)
#
# Related module: Sequel::Dataset::NoAutoLiteralStrings

#
module Sequel
  class Dataset
    module NoAutoLiteralStrings
      # Raise an error if passing a plain string or an array whose first
      # entry is a plain string.
      def filter_expr(expr = nil)
        case expr
        when LiteralString
          super
        when String
          raise Error, "plain string passed to a dataset filtering method"
        when Array
          if expr.first.is_a?(String) && !expr.first.is_a?(LiteralString)
            raise Error, "plain string passed to a dataset filtering method"
          end
          super
        else
          super
        end
      end

      # Raise an error if passing a plain string.
      def update_sql(values=OPTS)
        case values
        when LiteralString
          super
        when String
          raise Error, "plain string passed to a dataset filtering method"
        else
          super
        end
      end
    end

    register_extension(:no_auto_literal_strings, NoAutoLiteralStrings)
  end
end
