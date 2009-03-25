# Sequel extends the Array class to add methods to implement the SQL DSL.
# Most of these methods require that the array not be empty and that it
# must consist solely of other arrays that have exactly two elements.
class Array
  # Return a Sequel::SQL::BooleanExpression created from this array, not matching all of the
  # conditions.
  #
  #   ~[[:a, true]] # SQL: a IS NOT TRUE
  #   ~[[:a, 1], [:b, [2, 3]]] # SQL: a != 1 OR b NOT IN (2, 3)
  def ~
    sql_expr_if_all_two_pairs(:OR, true)
  end

  # True if the array is not empty and all of its elements are
  # arrays of size 2, false otherwise.  This is used to determine if the array
  # could be a specifier of conditions, used similarly to a hash
  # but allowing for duplicate keys and a specific order.
  #
  #    [].to_a.all_two_pairs? # => false
  #    [:a].to_a.all_two_pairs? # => false
  #    [[:b]].to_a.all_two_pairs? # => false
  #    [[:a, 1]].to_a.all_two_pairs? # => true
  def all_two_pairs?
    !empty? && all?{|i| (Array === i) && (i.length == 2)}
  end

  # Return a Sequel::SQL::CaseExpression with this array as the conditions and the given
  # default value and expression.
  #
  #   [[{:a=>[2,3]}, 1]].case(0) # SQL: CASE WHEN a IN (2, 3) THEN 1 ELSE 0 END
  #   [[:a, 1], [:b, 2]].case(:d, :c) # SQL: CASE c WHEN a THEN 1 WHEN b THEN 2 ELSE d END
  def case(default, expression = nil)
    ::Sequel::SQL::CaseExpression.new(self, default, expression)
  end

  # Return a Sequel::SQL::Array created from this array.  Used if this array contains
  # all two pairs and you want it treated as an SQL array instead of a ordered hash-like
  # conditions.
  #
  #   [[1, 2], [3, 4]] # SQL: 1 = 2 AND 3 = 4
  #   [[1, 2], [3, 4]].sql_array # SQL: ((1, 2), (3, 4))
  def sql_array
    ::Sequel::SQL::SQLArray.new(self)
  end

  # Return a Sequel::SQL::BooleanExpression created from this array, matching all of the
  # conditions.  Rarely do you need to call this explicitly, as Sequel generally
  # assumes that arrays of all two pairs specify this type of condition.
  #
  #   [[:a, true]].sql_expr # SQL: a IS TRUE
  #   [[:a, 1], [:b, [2, 3]]].sql_expr # SQL: a = 1 AND b IN (2, 3)
  def sql_expr
    sql_expr_if_all_two_pairs
  end

  # Return a Sequel::SQL::BooleanExpression created from this array, matching none
  # of the conditions.
  #
  #   [[:a, true]].sql_negate # SQL: a IS NOT TRUE
  #   [[:a, 1], [:b, [2, 3]]].sql_negate # SQL: a != 1 AND b NOT IN (2, 3)
  def sql_negate
    sql_expr_if_all_two_pairs(:AND, true)
  end

  # Return a Sequel::SQL::BooleanExpression created from this array, matching any of the
  # conditions.
  #
  #   [[:a, true]].sql_or # SQL: a IS TRUE
  #   [[:a, 1], [:b, [2, 3]]].sql_or # SQL: a = 1 OR b IN (2, 3)
  def sql_or
    sql_expr_if_all_two_pairs(:OR)
  end

  # Return a Sequel::SQL::BooleanExpression representing an SQL string made up of the
  # concatenation of this array's elements.  If an argument is passed
  # it is used in between each element of the array in the SQL
  # concatenation.  This does not require that the array be made up of all two pairs.
  #
  #   [:a].sql_string_join # SQL: a
  #   [:a, :b].sql_string_join # SQL: a || b
  #   [:a, 'b'].sql_string_join # SQL: a || 'b'
  #   ['a', :b].sql_string_join(' ') # SQL: 'a' || ' ' || b
  def sql_string_join(joiner=nil)
    if joiner
      args = zip([joiner]*length).flatten
      args.pop
    else
      args = self
    end
    args = args.collect{|a| [Symbol, ::Sequel::SQL::Expression, ::Sequel::LiteralString, TrueClass, FalseClass, NilClass].any?{|c| a.is_a?(c)} ? a : a.to_s}
    ::Sequel::SQL::StringExpression.new(:'||', *args)
  end

  private

  # Raise an error if this array is not made up of all two pairs, otherwise create a Sequel::SQL::BooleanExpression from this array.
  def sql_expr_if_all_two_pairs(*args)
    raise(Sequel::Error, 'Not all elements of the array are arrays of size 2, so it cannot be converted to an SQL expression') unless all_two_pairs?
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, *args)
  end
end

# Sequel extends the Hash class to add methods to implement the SQL DSL.
class Hash
  # Return a Sequel::SQL::BooleanExpression created from this hash, matching
  # all of the conditions in this hash and the condition specified by
  # the given argument.
  #
  #   {:a=>1} & :b # SQL: a = 1 AND b
  #   {:a=>true} & ~:b # SQL: a IS TRUE AND NOT b
  def &(ce)
    ::Sequel::SQL::BooleanExpression.new(:AND, self, ce)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching
  # all of the conditions in this hash or the condition specified by
  # the given argument.
  #
  #   {:a=>1} | :b # SQL: a = 1 OR b
  #   {:a=>true} | ~:b # SQL: a IS TRUE OR NOT b
  def |(ce)
    ::Sequel::SQL::BooleanExpression.new(:OR, self, ce)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, not matching all of the
  # conditions.
  #
  #   ~{:a=>true} # SQL: a IS NOT TRUE
  #   ~{:a=>1, :b=>[2, 3]} # SQL: a != 1 OR b NOT IN (2, 3)
  def ~
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR, true)
  end

  # Return a Sequel::SQL::CaseExpression with this hash as the conditions and the given
  # default value.  Note that the order of the conditions will be arbitrary, so all
  # conditions should be orthogonal.
  #
  #   {{:a=>[2,3]}=>1}.case(0) # SQL: CASE WHEN a IN (2, 3) THEN 1 ELSE 0 END
  #   {:a=>1, {:b=>2}].case(:d, :c) # SQL: CASE c WHEN a THEN 1 WHEN b THEN 2 ELSE d END
  #                                 #  or: CASE c WHEN b THEN 2 WHEN a THEN 1 ELSE d END
  def case(default, expression = nil)
    ::Sequel::SQL::CaseExpression.new(to_a, default, expression)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching all of the
  # conditions.  Rarely do you need to call this explicitly, as Sequel generally
  # assumes that hashes specify this type of condition.
  #
  #   {:a=>true}.sql_expr # SQL: a IS TRUE
  #   {:a=>1, :b=>[2, 3]}.sql_expr # SQL: a = 1 AND b IN (2, 3)
  def sql_expr
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching none
  # of the conditions.
  #
  #   {:a=>true}.sql_negate # SQL: a IS NOT TRUE
  #   {:a=>1, :b=>[2, 3]}.sql_negate # SQL: a != 1 AND b NOT IN (2, 3)
  def sql_negate
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :AND, true)
  end

  # Return a Sequel::SQL::BooleanExpression created from this hash, matching any of the
  # conditions.
  #
  #   {:a=>true}.sql_or # SQL: a IS TRUE
  #   {:a=>1, :b=>[2, 3]}.sql_or # SQL: a = 1 OR b IN (2, 3)
  def sql_or
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR)
  end
end

# Sequel extends the String class to add methods to implement the SQL DSL.
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
  
  # Returns a Sequel::SQL::Blob that holds the same data as this string. Blobs provide proper
  # escaping of binary data.
  def to_sequel_blob
    ::Sequel::SQL::Blob.new(self)
  end
end

# Sequel extends the Symbol class to add methods to implement the SQL DSL.
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
  # 
  #   :table.* # SQL: table.*
  #   :column * 2 # SQL: column * 2
  def *(ce=(arg=false;nil))
    return super(ce) unless arg == false
    Sequel::SQL::ColumnAll.new(self);
  end

  # Returns a Sequel::SQL::Function  with this as the function name,
  # and the given arguments. This is aliased as Symbol#[] if the RUBY_VERSION
  # is less than 1.9.0. Ruby 1.9 defines Symbol#[], and Sequel
  # doesn't override methods defined by ruby itself.
  #
  #   :now.sql_function # SQL: now()
  #   :sum.sql_function(:a) # SQL: sum(a)
  #   :concat.sql_function(:a, :b) # SQL: concat(a, b)
  def sql_function(*args)
    Sequel::SQL::Function.new(self, *args)
  end
  alias_method(:[], :sql_function) if RUBY_VERSION < '1.9.0'

  # Return an SQL array subscript with the given arguments.
  #
  #   :array.sql_subscript(1) # SQL: array[1]
  #   :array.sql_subscript(1, 2) # SQL: array[1, 2]
  def sql_subscript(*sub)
    Sequel::SQL::Subscript.new(self, sub.flatten)
  end
end
