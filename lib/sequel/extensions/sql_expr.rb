# The sql_expr extension adds the sql_expr method to every object, which
# returns an object that works nicely with Sequel's DSL.  This is
# best shown by example:
#
#   1.sql_expr < :a     # 1 < a
#   false.sql_expr & :a # FALSE AND a
#   true.sql_expr | :a  # TRUE OR a
#   ~nil.sql_expr       # NOT NULL
#   "a".sql_expr + "b"  # 'a' || 'b'

module Sequel
  module SQL
    # The GenericComplexExpression acts like a 
    # GenericExpression in terms of methods,
    # but has an internal structure of a
    # ComplexExpression.  It is used by Object#sql_expr.
    # Since we don't know what specific type of object
    # we are dealing with it, we treat it similarly to
    # how we treat symbols or literal strings, allowing
    # many different types of methods.
    class GenericComplexExpression < ComplexExpression
      include AliasMethods
      include BooleanMethods
      include CastMethods
      include ComplexExpressionMethods
      include InequalityMethods
      include NumericMethods
      include OrderMethods
      include StringMethods
      include SubscriptMethods
    end
  end
end

class Object
  # Return a copy of the object wrapped in a
  # Sequel::SQL::GenericComplexExpression.  Allows easy use
  # of the Object with Sequel's DSL.  You'll probably have
  # to make sure that Sequel knows how to literalize the
  # object properly, though.
  def sql_expr
    Sequel::SQL::GenericComplexExpression.new(:NOOP, self)
  end
end

class FalseClass
  # Returns a copy of the object wrapped in a
  # Sequel::SQL::BooleanExpression, allowing easy use
  # of Sequel's DSL:
  #
  #   false.sql_expr & :a  # FALSE AND a
  def sql_expr
    Sequel::SQL::BooleanExpression.new(:NOOP, self)
  end
end

class NilClass
  # Returns a copy of the object wrapped in a
  # Sequel::SQL::BooleanExpression, allowing easy use
  # of Sequel's DSL:
  #
  #   ~nil.sql_expr  # NOT NULL
  def sql_expr
    Sequel::SQL::BooleanExpression.new(:NOOP, self)
  end
end

class Numeric
  # Returns a copy of the object wrapped in a
  # Sequel::SQL::NumericExpression, allowing easy use
  # of Sequel's DSL:
  #
  #   1.sql_expr < :a  # 1 < a
  def sql_expr
    Sequel::SQL::NumericExpression.new(:NOOP, self)
  end
end

class Proc
  # Evaluates the proc as a virtual row block.
  # If a hash or array of two element arrays is returned,
  # they are converted to a Sequel::SQL::BooleanExpression.  Otherwise,
  # unless the object returned is already an Sequel::SQL::Expression,
  # convert the object to an Sequel::SQL::GenericComplexExpression.
  #
  #   proc{a(b)}.sql_expr + 1  # a(b) + 1
  #   proc{{a=>b}}.sql_expr | true  # (a = b) OR TRUE
  #   proc{1}.sql_expr + :a  # 1 + a
  def sql_expr
    o = Sequel.virtual_row(&self)
    if Sequel.condition_specifier?(o)
      Sequel::SQL::BooleanExpression.from_value_pairs(o, :AND)
    elsif o.is_a?(Sequel::SQL::Expression)
      o
    else
      Sequel::SQL::GenericComplexExpression.new(:NOOP, o)
    end
  end
end

class String
  # Returns a copy of the object wrapped in a
  # Sequel::SQL::StringExpression, allowing easy use
  # of Sequel's DSL:
  #
  #   "a".sql_expr + :a  # 'a' || a
  def sql_expr
    Sequel::SQL::StringExpression.new(:NOOP, self)
  end
end

class TrueClass
  # Returns a copy of the object wrapped in a
  # Sequel::SQL::BooleanExpression, allowing easy use
  # of Sequel's DSL:
  #
  #   true.sql_expr | :a  # TRUE OR a
  def sql_expr
    Sequel::SQL::BooleanExpression.new(:NOOP, self)
  end
end

