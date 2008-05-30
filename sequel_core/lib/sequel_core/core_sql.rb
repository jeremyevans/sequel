# This file holds the extensions to core ruby classes that relate to the creation of SQL
# code.

class Array
  # Return a Sequel::SQL::ComplexExpression created from this array, not matching any of the
  # conditions.
  def ~
    sql_expr_if_all_two_pairs(:OR, true)
  end

  # Return a Sequel::SQL::ComplexExpression created from this array, matching all of the
  # conditions.
  def sql_expr
    sql_expr_if_all_two_pairs
  end

  # Return a Sequel::SQL::ComplexExpression created from this array, matching none
  # of the conditions.
  def sql_negate
    sql_expr_if_all_two_pairs(:AND, true)
  end

  # Return a Sequel::SQL::ComplexExpression created from this array, matching any of the
  # conditions.
  def sql_or
    sql_expr_if_all_two_pairs(:OR)
  end

  # Return a Sequel::SQL::ComplexExpression representing an SQL string made up of the
  # concatenation of this array's elements.  If an argument is passed
  # it is used in between each element of the array in the SQL
  # concatenation.
  def sql_string_join(joiner=nil)
    if joiner
      args = self.inject([]) do |m, a|
        m << a
        m << joiner
      end
      args.pop
    else
      args = self
    end
    args = args.collect{|a| a.is_one_of?(Symbol, ::Sequel::SQL::Expression, ::Sequel::LiteralString, TrueClass, FalseClass, NilClass) ? a : a.to_s}
    ::Sequel::SQL::ComplexExpression.new(:'||', *args)
  end

  # Concatenates an array of strings into an SQL string. ANSI SQL and C-style
  # comments are removed, as well as excessive white-space.
  def to_sql
    map {|l| ((m = /^(.*)--/.match(l)) ? m[1] : l).chomp}.join(' '). \
      gsub(/\/\*.*\*\//, '').gsub(/\s+/, ' ').strip
  end

  private

  # Raise an error if this array is not made up of all two pairs, otherwise create a Sequel::SQL::ComplexExpression from this array.
  def sql_expr_if_all_two_pairs(*args)
    raise(Sequel::Error, 'Not all elements of the array are arrays of size 2, so it cannot be converted to an SQL expression') unless all_two_pairs?
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, *args)
  end
end

class Hash
  # Return a Sequel::SQL::ComplexExpression created from this hash, matching
  # all of the conditions in this hash and the condition specified by
  # the given argument.
  def &(ce)
    ::Sequel::SQL::ComplexExpression.new(:AND, self, ce)
  end

  # Return a Sequel::SQL::ComplexExpression created from this hash, matching
  # all of the conditions in this hash or the condition specified by
  # the given argument.
  def |(ce)
    ::Sequel::SQL::ComplexExpression.new(:OR, self, ce)
  end

  # Return a Sequel::SQL::ComplexExpression created from this hash, not matching any of the
  # conditions.
  def ~
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, :OR, true)
  end

  # Return a Sequel::SQL::ComplexExpression created from this hash, matching all of the
  # conditions.
  def sql_expr
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self)
  end

  # Return a Sequel::SQL::ComplexExpression created from this hash, matching none
  # of the conditions.
  def sql_negate
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, :AND, true)
  end

  # Return a Sequel::SQL::ComplexExpression created from this hash, matching any of the
  # conditions.
  def sql_or
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, :OR)
  end
end

class String
  include Sequel::SQL::ColumnMethods

  # Converts a string into an LiteralString, in order to override string
  # literalization, e.g.:
  #
  #   DB[:items].filter(:abc => 'def').sql #=>
  #     "SELECT * FROM items WHERE (abc = 'def')"
  #
  #   DB[:items].filter(:abc => 'def'.lit).sql #=>
  #     "SELECT * FROM items WHERE (abc = def)"
  #
  def lit
    Sequel::LiteralString.new(self)
  end
  alias_method :expr, :lit
  
  # Splits a string into separate SQL statements, removing comments
  # and excessive white-space.
  def split_sql
    to_sql.split(';').map {|s| s.strip}
  end

  # Converts a string into an SQL string by removing comments.
  # See also Array#to_sql.
  def to_sql
    split("\n").to_sql
  end
end

class Symbol
  include Sequel::SQL::ColumnMethods
  include Sequel::SQL::ComplexExpressionMethods

  # If no argument is given, returns a Sequel::SQL::ColumnAll object specifying all
  # columns for this table.
  # If an argument is given, returns a Sequel::SQL::ComplexExpression using the *
  # (multiplication) operator with this and the given argument.
  def *(ce=(arg=false;nil))
    return super(ce) unless arg == false
    Sequel::SQL::ColumnAll.new(self);
  end

  # Returns a Sequel::SQL::Function  with this as the function name,
  # and the given arguments.
  def [](*args)
    Sequel::SQL::Function.new(self, *args)
  end

  # If the given argument is an Integer or an array containing an Integer, returns
  # a Sequel::SQL::Subscript with this column and the given arg.
  # Otherwise returns a Sequel::SQL::ComplexExpression where this column (which should be boolean)
  # or the given argument is true.
  def |(sub)
    return super unless (Integer === sub) || ((Array === sub) && sub.any?{|x| Integer === x})
    Sequel::SQL::Subscript.new(self, Array(sub))
  end
  
  # Delegate the creation of the resulting SQL to the given dataset,
  # since it may be database dependent.
  def to_column_ref(ds)
    ds.symbol_to_column_ref(self)
  end
end
