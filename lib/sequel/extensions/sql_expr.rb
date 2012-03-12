# The sql_expr extension adds the sql_expr method to every object, which
# returns an wrapped object that works nicely with Sequel's DSL.  This is
# best shown by example:
#
#   1.sql_expr < :a     # 1 < a
#   false.sql_expr & :a # FALSE AND a
#   true.sql_expr | :a  # TRUE OR a
#   ~nil.sql_expr       # NOT NULL
#   "a".sql_expr + "b"  # 'a' || 'b'

class Object
  # Return a copy of the object wrapped in a
  # Sequel::SQL::Wrapper.  Allows easy use
  # of the Object with Sequel's DSL.  You'll probably have
  # to make sure that Sequel knows how to literalize the
  # object properly, though.
  def sql_expr
    Sequel::SQL::Wrapper.new(self)
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
  # wrap the object in an Sequel::SQL::Wrapper.
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
      Sequel::SQL::Wrapper.new(o)
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

