# The pg_array_ops extension adds support to Sequel's DSL to make
# it easier to call PostgreSQL array functions and operators. The
# most common usage is taking an object that represents an SQL
# identifier (such as a :symbol), and calling #pg_array on it:
#
#   ia = :int_array_column.pg_array
#
# This creates a Sequel::Postgres::ArrayOp object that can be used
# for easier querying:
#
#   ia[1]     # int_array_column[1]
#   ia[1][2]  # int_array_column[1][2]
#
#   ia.contains(:other_int_array_column)     # @> 
#   ia.contained_by(:other_int_array_column) # <@
#   ia.overlaps(:other_int_array_column)     # &&
#   ia.concat(:other_int_array_column)       # ||
#
#   ia.push(1)         # int_array_column || 1
#   ia.unshift(1)      # 1 || int_array_column
#
#   ia.any             # ANY(int_array_column)
#   ia.all             # ALL(int_array_column)
#   ia.dims            # array_dims(int_array_column)
#   ia.length          # array_length(int_array_column, 1)
#   ia.length(2)       # array_length(int_array_column, 2)
#   ia.lower           # array_lower(int_array_column, 1)
#   ia.lower(2)        # array_lower(int_array_column, 2)
#   ia.join            # array_to_string(int_array_column, '', NULL)
#   ia.join(':')       # array_to_string(int_array_column, ':', NULL)
#   ia.join(':', ' ')  # array_to_string(int_array_column, ':', ' ')
#   ia.unnest          # unnest(int_array_column)
# 
# See the PostgreSQL array function and operator documentation for more
# details on what these functions and operators do.
#
# If you are also using the pg_array extension, you should load it before
# loading this extension.  Doing so will allow you to use PGArray#op to get
# an ArrayOp, allowing you to perform array operations on array literals.
module Sequel
  module Postgres
    # The ArrayOp class is a simple container for a single object that
    # defines methods that yield Sequel expression objects representing
    # PostgreSQL array operators and functions.
    #
    # In the method documentation examples, assume that:
    #
    #   array_op = :array.pg_array
    class ArrayOp < Sequel::SQL::Wrapper
      CONCAT = ["(".freeze, " || ".freeze, ")".freeze].freeze
      CONTAINS = ["(".freeze, " @> ".freeze, ")".freeze].freeze
      CONTAINED_BY = ["(".freeze, " <@ ".freeze, ")".freeze].freeze
      OVERLAPS = ["(".freeze, " && ".freeze, ")".freeze].freeze

      # Access a member of the array, returns an SQL::Subscript instance:
      #
      #   array_op[1] # array[1]
      def [](key)
        Sequel::SQL::Subscript.new(self, [key])
      end

      # Call the ALL function:
      #
      #   array_op.all # ALL(array)
      #
      # Usually used like:
      #
      #   dataset.where(1=>array_op.all)
      #   # WHERE (1 = ALL(array))
      def all
        function(:ALL)
      end

      # Call the ANY function:
      #
      #   array_op.all # ANY(array)
      #
      # Usually used like:
      #
      #   dataset.where(1=>array_op.any)
      #   # WHERE (1 = ANY(array))
      def any
        function(:ANY)
      end

      # Use the contains (@>) operator:
      #
      #   array_op.contains(:a) # (array @> a)
      def contains(other)
        bool_op(CONTAINS, other)
      end

      # Use the contained by (<@) operator:
      #
      #   array_op.contained_by(:a) # (array <@ a)
      def contained_by(other)
        bool_op(CONTAINED_BY, other)
      end

      # Call the array_dims method:
      #
      #   array_op.dims # array_dims(array)
      def dims
        function(:array_dims)
      end

      # Call the array_length method:
      #
      #   array_op.length    # array_length(array, 1)
      #   array_op.length(2) # array_length(array, 2)
      def length(dimension = 1)
        function(:array_length, dimension)
      end
      
      # Call the array_lower method:
      #
      #   array_op.lower    # array_lower(array, 1)
      #   array_op.lower(2) # array_lower(array, 2)
      def lower(dimension = 1)
        function(:array_lower, dimension)
      end
      
      # Use the overlaps (&&) operator:
      #
      #   array_op.overlaps(:a) # (array && a)
      def overlaps(other)
        bool_op(OVERLAPS, other)
      end

      # Use the concatentation (||) operator:
      #
      #   array_op.push(:a) # (array || a)
      #   array_op.concat(:a) # (array || a)
      def push(other)
        array_op(CONCAT, [self, other])
      end
      alias concat push

      # Return the receiver.
      def pg_array
        self
      end

      # Call the array_to_string method:
      #
      #   array_op.join           # array_to_string(array, '', NULL)
      #   array_op.to_string      # array_to_string(array, '', NULL)
      #   array_op.join(":")      # array_to_string(array, ':', NULL)
      #   array_op.join(":", "*") # array_to_string(array, ':', '*')
      def to_string(joiner="", null=nil)
        function(:array_to_string, joiner, null)
      end
      alias join to_string
      
      # Call the unnest method:
      #
      #   array_op.unnest # unnest(array)
      def unnest
        function(:unnest)
      end
      
      # Use the concatentation (||) operator, reversing the order:
      #
      #   array_op.unshift(:a) # (a || array)
      def unshift(other)
        array_op(CONCAT, [other, self])
      end

      private

      # Return a placeholder literal with the given str and args, wrapped
      # in an ArrayOp, used by operators that return arrays.
      def array_op(str, args)
        ArrayOp.new(Sequel::SQL::PlaceholderLiteralString.new(str, args))
      end

      # Return a placeholder literal with the given str and args, wrapped
      # in a boolean expression, used by operators that return booleans.
      def bool_op(str, other)
        Sequel::SQL::BooleanExpression.new(:NOOP, Sequel::SQL::PlaceholderLiteralString.new(str, [value, other]))
      end

      # Return a function with the given name, and the receiver as the first
      # argument, with any additional arguments given.
      def function(name, *args)
        SQL::Function.new(name, self, *args)
      end
    end

    module ArrayOpMethods
      # Wrap the receiver in an ArrayOp so you can easily use the PostgreSQL
      # array functions and operators with it.
      def pg_array
        ArrayOp.new(self)
      end
    end

    if defined?(PGArray)
      class PGArray
        # Wrap the PGArray instance in an ArrayOp, allowing you to easily use
        # the PostgreSQL array functions and operators with literal arrays.
        def op
          ArrayOp.new(self)
        end
      end
    end
  end

  class SQL::GenericExpression
    include Sequel::Postgres::ArrayOpMethods
  end

  class LiteralString
    include Sequel::Postgres::ArrayOpMethods
  end
end

class Symbol
  include Sequel::Postgres::ArrayOpMethods
end
