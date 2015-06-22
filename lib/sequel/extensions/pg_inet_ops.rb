# The pg_inet_ops extension adds support to Sequel's DSL to make
# it easier to call PostgreSQL inet functions and operators.
#
# To load the extension:
#
#   Sequel.extension :pg_inet_ops
#
# The most common usage is passing an expression to Sequel.pg_inet_op:
#
#   r = Sequel.pg_inet_op(:inet)
#
# If you have also loaded the pg_inet extension, you can use
# Sequel.pg_inet as well:
#
#   r = Sequel.pg_inet(:inet)
#
# Also, on most Sequel expression objects, you can call the pg_inet
# method:
#
#   r = Sequel.expr(:ip).pg_inet
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Symbol#pg_inet:
#
#   r = :inet.pg_inet
#
# This creates a Sequel::Postgres::InetOp object that can be used
# for easier querying:
#
#   r.less_than(:other)                        # inet < other
#   r.less_than_or_equal(:other)               # inet <= other
#   r.equals(:other)                           # inet = other
#   r.greater_than_or_equal(:other)            # inet >= other
#   r.greater_than(:other)                     # inet > other
#   r.not_equal(:other)                        # inet <> other
#   r.contained_by(:other)                     # inet << other
#   r.contained_by_or_equals(:other)           # inet <<= other
#   r.contains(:other)                         # inet >> other
#   r.contains_or_equals(:other)               # inet >>= other
#   r.contains_or_contained_by(:other)         # inet && other
#
#   r.abbrev           # abbrev(inet)
#   r.broadcast        # broadcast(inet)
#   r.family           # family(inet)
#   r.host             # host(inet)
#   r.hostmask         # hostmask(inet)
#   r.masklen          # masklen(inet)
#   r.netmask          # netmask(inet)
#   r.network          # network(inet)
#   r.text             # text(inet)
#
# See the PostgreSQL network function and operator documentation for more
# details on what these functions and operators do.
#
module Sequel
  module Postgres
    # The InetOp class is a simple container for a single object that
    # defines methods that yield Sequel expression objects representing
    # PostgreSQL inet operators and functions.
    #
    # Most methods in this class are defined via metaprogramming, see
    # the pg_inet_ops extension documentation for details on the API.
    class InetOp < Sequel::SQL::Wrapper
      OPERATORS = {
        :less_than => ["(".freeze, " < ".freeze, ")".freeze].freeze,
        :less_than_or_equal => ["(".freeze, " <= ".freeze, ")".freeze].freeze,
        :equals => ["(".freeze, " = ".freeze, ")".freeze].freeze,
        :greater_than_or_equal => ["(".freeze, " >= ".freeze, ")".freeze].freeze,
        :greater_than => ["(".freeze, " > ".freeze, ")".freeze].freeze,
        :not_equal => ["(".freeze, " <> ".freeze, ")".freeze].freeze,
        :contained_by => ["(".freeze, " << ".freeze, ")".freeze].freeze,
        :contained_by_or_equals => ["(".freeze, " <<= ".freeze, ")".freeze].freeze,
        :contains => ["(".freeze, " >> ".freeze, ")".freeze].freeze,
        :contains_or_equals => ["(".freeze, " >>= ".freeze, ")".freeze].freeze,
        :contains_or_contained_by => ["(".freeze, " && ".freeze, ")".freeze].freeze,
      }
      FUNCTIONS = %w'abbrev broadcast family host hostmask netmask network text'

      FUNCTIONS.each do |f|
        class_eval("def #{f}; function(:#{f}) end", __FILE__, __LINE__)
      end
      OPERATORS.keys.each do |f|
        class_eval("def #{f}(v); operator(:#{f}, v) end", __FILE__, __LINE__)
      end

      # Return the receiver.
      def pg_inet
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

    module InetOpMethods
      # Wrap the receiver in an InetOp so you can easily use the PostgreSQL
      # inet functions and operators with it.
      def pg_inet
        InetOp.new(self)
      end
    end

    module SQL::Builders
      # Return the expression wrapped in the Postgres::InetOp.
      def pg_inet_op(v)
        case v
        when Postgres::InetOp
          v
        else
          Postgres::InetOp.new(v)
        end
      end
    end

    class SQL::GenericExpression
      include Sequel::Postgres::InetOpMethods
    end

    class LiteralString
      include Sequel::Postgres::InetOpMethods
    end
  end
end

# :nocov:
if Sequel.core_extensions?
  class Symbol
    include Sequel::Postgres::InetOpMethods
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Symbol do
      include Sequel::Postgres::InetOpMethods
    end
  end
end
# :nocov:
