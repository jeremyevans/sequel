# The pg_range_ops extension adds support to Sequel's DSL to make
# it easier to call PostgreSQL range functions and operators.
#
# To load the extension:
#
#   Sequel.extension :pg_range_ops
#
# The most common usage is taking an object that represents an SQL
# identifier (such as a :symbol), and calling #pg_range on it:
#
#   r = :range.pg_range
#
# This creates a Sequel::Postgres::RangeOp object that can be used
# for easier querying:
#
#   r.contains(:other)      # range @> other
#   r.contained_by(:other)  # range <@ other
#   r.overlaps(:other)      # range && other
#   r.left_of(:other)       # range << other
#   r.right_of(:other)      # range >> other
#   r.starts_before(:other) # range &< other
#   r.ends_after(:other)    # range &> other
#   r.adjacent_to(:other)   # range -|- other
#
#   r.lower            # lower(range)
#   r.upper            # upper(range)
#   r.isempty          # isempty(range)
#   r.lower_inc        # lower_inc(range)
#   r.upper_inc        # upper_inc(range)
#   r.lower_inf        # lower_inf(range)
#   r.upper_inf        # upper_inf(range)
#
# See the PostgreSQL range function and operator documentation for more
# details on what these functions and operators do.
#
# If you are also using the pg_range extension, you should load it before
# loading this extension.  Doing so will allow you to use PGArray#op to get
# an RangeOp, allowing you to perform range operations on range literals.

module Sequel
  module Postgres
    # The RangeOp class is a simple container for a single object that
    # defines methods that yield Sequel expression objects representing
    # PostgreSQL range operators and functions.
    #
    # Most methods in this class are defined via metaprogramming, see
    # the pg_range_ops extension documentation for details on the API.
    class RangeOp < Sequel::SQL::Wrapper
      OPERATORS = {
        :contains => ["(".freeze, " @> ".freeze, ")".freeze].freeze,
        :contained_by => ["(".freeze, " <@ ".freeze, ")".freeze].freeze,
        :left_of => ["(".freeze, " << ".freeze, ")".freeze].freeze,
        :right_of => ["(".freeze, " >> ".freeze, ")".freeze].freeze,
        :starts_before => ["(".freeze, " &< ".freeze, ")".freeze].freeze,
        :ends_after => ["(".freeze, " &> ".freeze, ")".freeze].freeze,
        :adjacent_to => ["(".freeze, " -|- ".freeze, ")".freeze].freeze,
        :overlaps => ["(".freeze, " && ".freeze, ")".freeze].freeze,
      }
      FUNCTIONS = %w'lower upper isempty lower_inc upper_inc lower_inf upper_inf'

      FUNCTIONS.each do |f|
        class_eval("def #{f}; function(:#{f}) end", __FILE__, __LINE__)
      end
      OPERATORS.keys.each do |f|
        class_eval("def #{f}(v); operator(:#{f}, v) end", __FILE__, __LINE__)
      end

      # These operators are already supported by the wrapper, but for ranges they
      # return ranges, so wrap the results in another RangeOp.
      %w'+ * -'.each do |f|
        class_eval("def #{f}(v); RangeOp.new(super) end", __FILE__, __LINE__)
      end

      # Return the receiver.
      def pg_range
        self
      end

      private

      # Create a boolen expression for the given type and argument.
      def operator(type, other)
        Sequel::SQL::BooleanExpression.new(:NOOP, Sequel::SQL::PlaceholderLiteralString.new(OPERATORS[type], [value, other]))
      end

      # Return a function called with the receiver.
      def function(name)
        Sequel::SQL::Function.new(name, self)
      end
    end

    module RangeOpMethods
      # Wrap the receiver in an RangeOp so you can easily use the PostgreSQL
      # range functions and operators with it.
      def pg_range
        RangeOp.new(self)
      end
    end

    if defined?(PGRange)
      class PGRange
        # Wrap the PGRange instance in an RangeOp, allowing you to easily use
        # the PostgreSQL range functions and operators with literal ranges.
        def op
          RangeOp.new(self)
        end
      end
    end
  end

  class SQL::GenericExpression
    include Sequel::Postgres::RangeOpMethods
  end

  class LiteralString
    include Sequel::Postgres::RangeOpMethods
  end
end

class Symbol
  include Sequel::Postgres::RangeOpMethods
end
