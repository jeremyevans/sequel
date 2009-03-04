class Array
  # Return a Sequel::SQL::BooleanExpression created from this array, not matching any of the
  # conditions.
  def ~
    sql_expr_if_all_two_pairs(:OR, true)
  end

  # Return a Sequel::SQL::CaseExpression with this array as the conditions and the given
  # default value.
  def case(default, expression = nil)
    ::Sequel::SQL::CaseExpression.new(self, default, expression)
  end

  # Return a Sequel::SQL::Array created from this array.  Used if this array contains
  # all two pairs and you want it treated as an SQL array instead of a ordered hash-like
  # conditions.
  def sql_array
    ::Sequel::SQL::SQLArray.new(self)
  end

  # Return a Sequel::SQL::BooleanExpression created from this array, matching all of the
  # conditions.
  def sql_expr
    sql_expr_if_all_two_pairs
  end

  # Return a Sequel::SQL::BooleanExpression created from this array, matching none
  # of the conditions.
  def sql_negate
    sql_expr_if_all_two_pairs(:AND, true)
  end

  # Return a Sequel::SQL::BooleanExpression created from this array, matching any of the
  # conditions.
  def sql_or
    sql_expr_if_all_two_pairs(:OR)
  end

  # Return a Sequel::SQL::BooleanExpression representing an SQL string made up of the
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
    ::Sequel::SQL::StringExpression.new(:'||', *args)
  end

  private

  # Raise an error if this array is not made up of all two pairs, otherwise create a Sequel::SQL::BooleanExpression from this array.
  def sql_expr_if_all_two_pairs(*args)
    raise(Sequel::Error, 'Not all elements of the array are arrays of size 2, so it cannot be converted to an SQL expression') unless all_two_pairs?
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, *args)
  end
end

class Hash
  # Return a Sequel::SQL::BooleanExpression created from this hash, matching
  # all of the conditions in this hash and the condition specified by
  # the given argument.
  def &(ce)
    ::Sequel::SQL::BooleanExpression.new(:AND, self, ce)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching
  # all of the conditions in this hash or the condition specified by
  # the given argument.
  def |(ce)
    ::Sequel::SQL::BooleanExpression.new(:OR, self, ce)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, not matching any of the
  # conditions.
  def ~
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR, true)
  end

  # Return a Sequel::SQL::CaseExpression with this hash as the conditions and the given
  # default value.  Note that the order of the conditions will be arbitrary, so all
  # conditions should be orthogonal.
  def case(default, expression = nil)
    ::Sequel::SQL::CaseExpression.new(to_a, default, expression)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching all of the
  # conditions.
  def sql_expr
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching none
  # of the conditions.
  def sql_negate
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :AND, true)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching any of the
  # conditions.
  def sql_or
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR)
  end
end

class String
  include Sequel::SQL::AliasMethods
  include Sequel::SQL::CastMethods

  # Converts a string into a Sequel::LiteralString, in order to override string
  # literalization, e.g.:
  #
  #   DB[:items].filter(:abc => 'def').sql #=>
  #     "SELECT * FROM items WHERE (abc = 'def')"
  #
  #   DB[:items].filter(:abc => 'def'.lit).sql #=>
  #     "SELECT * FROM items WHERE (abc = def)"
  #
  # You can also provide arguments, to create a Sequel::SQL::PlaceholderLiteralString:
  #
  #    DB[:items].select{|o| o.count('DISTINCT ?'.lit(:a))}.sql #=>
  #      "SELECT count(DISTINCT a) FROM items"
  def lit(*args)
    args.empty? ? Sequel::LiteralString.new(self) : Sequel::SQL::PlaceholderLiteralString.new(self, args)
  end
  
  # Returns a Blob that holds the same data as this string. Blobs provide proper
  # escaping of binary data.
  def to_sequel_blob
    ::Sequel::SQL::Blob.new self
  end
  alias to_blob to_sequel_blob
end

class Symbol
  include Sequel::SQL::QualifyingMethods
  include Sequel::SQL::IdentifierMethods
  include Sequel::SQL::AliasMethods
  include Sequel::SQL::CastMethods
  include Sequel::SQL::OrderMethods
  include Sequel::SQL::BooleanMethods
  include Sequel::SQL::NumericMethods
  include Sequel::SQL::StringMethods
  include Sequel::SQL::ComplexExpressionMethods
  include Sequel::SQL::InequalityMethods if RUBY_VERSION < '1.9.0'

  # If no argument is given, returns a Sequel::SQL::ColumnAll object specifying all
  # columns for this table.
  # If an argument is given, returns a Sequel::SQL::NumericExpression using the *
  # (multiplication) operator with this and the given argument.
  def *(ce=(arg=false;nil))
    return super(ce) unless arg == false
    Sequel::SQL::ColumnAll.new(self);
  end

  # Returns a Sequel::SQL::Function  with this as the function name,
  # and the given arguments. This is aliased as Symbol#[] if ruby 1.9
  # is not being used.  ruby 1.9 includes Symbol#[], and Sequel
  # doesn't override methods defined by ruby itself.
  def sql_function(*args)
    Sequel::SQL::Function.new(self, *args)
  end
  alias_method(:[], :sql_function) if RUBY_VERSION < '1.9.0'

  # If the given argument is an Integer or an array containing an Integer, returns
  # a Sequel::SQL::Subscript with this column and the given arg.
  # Otherwise returns a Sequel::SQL::BooleanExpression where this column (which should be boolean)
  # or the given argument is true.
  def |(sub)
    return super unless (Integer === sub) || ((Array === sub) && sub.any?{|x| Integer === x})
    Sequel::SQL::Subscript.new(self, Array(sub))
  end
end
