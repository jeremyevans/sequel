# frozen-string-literal: true
#
# These are extensions to core classes that Sequel enables by default.
# They make using Sequel's DSL easier by adding methods to Array,
# Hash, String, and Symbol to add methods that return Sequel
# expression objects.
#
# This extension is currently loaded by default, but that will no
# longer be true in Sequel 4.  Starting in Sequel 4, you will
# need to load it manually via:
#
#   Sequel.extension :core_extensions

# This extension loads the core extensions.
def Sequel.core_extensions?
  true
end

# Sequel extends +Array+ to add methods to implement the SQL DSL.
# Most of these methods require that the array not be empty and that it
# must consist solely of other arrays that have exactly two elements.
class Array
  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this array, not matching all of the
  # conditions.
  #
  #   ~[[:a, true]] # SQL: a IS NOT TRUE
  #   ~[[:a, 1], [:b, [2, 3]]] # SQL: a != 1 OR b NOT IN (2, 3)
  def ~
    Sequel.~(self)
  end

  # Return a <tt>Sequel::SQL::CaseExpression</tt> with this array as the conditions and the given
  # default value and expression.
  #
  #   [[{:a=>[2,3]}, 1]].case(0) # SQL: CASE WHEN a IN (2, 3) THEN 1 ELSE 0 END
  #   [[:a, 1], [:b, 2]].case(:d, :c) # SQL: CASE c WHEN a THEN 1 WHEN b THEN 2 ELSE d END
  def case(*args)
    ::Sequel::SQL::CaseExpression.new(self, *args)
  end

  # Return a <tt>Sequel::SQL::ValueList</tt> created from this array.  Used if this array contains
  # all two element arrays and you want it treated as an SQL value list (IN predicate) 
  # instead of as a conditions specifier (similar to a hash).  This is not necessary if you are using
  # this array as a value in a filter, but may be necessary if you are using it as a
  # value with placeholder SQL:
  #
  #   DB[:a].filter([:a, :b]=>[[1, 2], [3, 4]]) # SQL: (a, b) IN ((1, 2), (3, 4))
  #   DB[:a].filter('(a, b) IN ?', [[1, 2], [3, 4]]) # SQL: (a, b) IN ((1 = 2) AND (3 = 4))
  #   DB[:a].filter('(a, b) IN ?', [[1, 2], [3, 4]].sql_value_list) # SQL: (a, b) IN ((1, 2), (3, 4))
  def sql_value_list
    ::Sequel::SQL::ValueList.new(self)
  end
  alias sql_array sql_value_list

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this array, matching all of the
  # conditions.  Rarely do you need to call this explicitly, as Sequel generally
  # assumes that arrays of two element arrays specify this type of condition.  One case where
  # it can be necessary to use this is if you are using the object as a value in a filter hash
  # and want to use the = operator instead of the IN operator (which is used by default for
  # arrays of two element arrays).
  #
  #   [[:a, true]].sql_expr # SQL: a IS TRUE
  #   [[:a, 1], [:b, [2, 3]]].sql_expr # SQL: a = 1 AND b IN (2, 3)
  def sql_expr
    Sequel.expr(self)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this array, matching none
  # of the conditions.
  #
  #   [[:a, true]].sql_negate # SQL: a IS NOT TRUE
  #   [[:a, 1], [:b, [2, 3]]].sql_negate # SQL: a != 1 AND b NOT IN (2, 3)
  def sql_negate
    Sequel.negate(self)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this array, matching any of the
  # conditions.
  #
  #   [[:a, true]].sql_or # SQL: a IS TRUE
  #   [[:a, 1], [:b, [2, 3]]].sql_or # SQL: a = 1 OR b IN (2, 3)
  def sql_or
    Sequel.or(self)
  end

  # Return a <tt>Sequel::SQL::StringExpression</tt> representing an SQL string made up of the
  # concatenation of this array's elements.  If an argument is passed
  # it is used in between each element of the array in the SQL
  # concatenation.
  #
  #   [:a].sql_string_join # SQL: a
  #   [:a, :b].sql_string_join # SQL: a || b
  #   [:a, 'b'].sql_string_join # SQL: a || 'b'
  #   ['a', :b].sql_string_join(' ') # SQL: 'a' || ' ' || b
  def sql_string_join(joiner=nil)
    Sequel.join(self, joiner)
  end
end

# Sequel extends +Hash+ to add methods to implement the SQL DSL.
class Hash
  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this hash, matching
  # all of the conditions in this hash and the condition specified by
  # the given argument.
  #
  #   {:a=>1} & :b # SQL: a = 1 AND b
  #   {:a=>true} & ~:b # SQL: a IS TRUE AND NOT b
  def &(ce)
    ::Sequel::SQL::BooleanExpression.new(:AND, self, ce)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this hash, matching
  # all of the conditions in this hash or the condition specified by
  # the given argument.
  #
  #   {:a=>1} | :b # SQL: a = 1 OR b
  #   {:a=>true} | ~:b # SQL: a IS TRUE OR NOT b
  def |(ce)
    ::Sequel::SQL::BooleanExpression.new(:OR, self, ce)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this hash, not matching all of the
  # conditions.
  #
  #   ~{:a=>true} # SQL: a IS NOT TRUE
  #   ~{:a=>1, :b=>[2, 3]} # SQL: a != 1 OR b NOT IN (2, 3)
  def ~
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR, true)
  end

  # Return a <tt>Sequel::SQL::CaseExpression</tt> with this hash as the conditions and the given
  # default value.  Note that the order of the conditions will be arbitrary on ruby 1.8, so all
  # conditions should be orthogonal.
  #
  #   {{:a=>[2,3]}=>1}.case(0) # SQL: CASE WHEN a IN (2, 3) THEN 1 ELSE 0 END
  #   {:a=>1, :b=>2}.case(:d, :c) # SQL: CASE c WHEN a THEN 1 WHEN b THEN 2 ELSE d END
  #                                 #  or: CASE c WHEN b THEN 2 WHEN a THEN 1 ELSE d END
  def case(*args)
    ::Sequel::SQL::CaseExpression.new(to_a, *args)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this hash, matching all of the
  # conditions.  Rarely do you need to call this explicitly, as Sequel generally
  # assumes that hashes specify this type of condition.
  #
  #   {:a=>true}.sql_expr # SQL: a IS TRUE
  #   {:a=>1, :b=>[2, 3]}.sql_expr # SQL: a = 1 AND b IN (2, 3)
  def sql_expr
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this hash, matching none
  # of the conditions.
  #
  #   {:a=>true}.sql_negate # SQL: a IS NOT TRUE
  #   {:a=>1, :b=>[2, 3]}.sql_negate # SQL: a != 1 AND b NOT IN (2, 3)
  def sql_negate
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :AND, true)
  end

  # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from this hash, matching any of the
  # conditions.
  #
  #   {:a=>true}.sql_or # SQL: a IS TRUE
  #   {:a=>1, :b=>[2, 3]}.sql_or # SQL: a = 1 OR b IN (2, 3)
  def sql_or
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR)
  end
end

# Sequel extends +String+ to add methods to implement the SQL DSL.
class String
  include Sequel::SQL::AliasMethods
  include Sequel::SQL::CastMethods

  # Converts a string into a <tt>Sequel::LiteralString</tt>, in order to override string
  # literalization, e.g.:
  #
  #   DB[:items].filter(:abc => 'def').sql #=>
  #     "SELECT * FROM items WHERE (abc = 'def')"
  #
  #   DB[:items].filter(:abc => 'def'.lit).sql #=>
  #     "SELECT * FROM items WHERE (abc = def)"
  #
  # You can also provide arguments, to create a <tt>Sequel::SQL::PlaceholderLiteralString</tt>:
  #
  #    DB[:items].select{|o| o.count('DISTINCT ?'.lit(:a))}.sql #=>
  #      "SELECT count(DISTINCT a) FROM items"
  def lit(*args)
    args.empty? ? Sequel::LiteralString.new(self) : Sequel::SQL::PlaceholderLiteralString.new(self, args)
  end
  
  # Returns a <tt>Sequel::SQL::Blob</tt> that holds the same data as this string. Blobs provide proper
  # escaping of binary data.
  def to_sequel_blob
    ::Sequel::SQL::Blob.new(self)
  end
end

# Sequel extends +Symbol+ to add methods to implement the SQL DSL.
class Symbol
  include Sequel::SQL::AliasMethods
  include Sequel::SQL::CastMethods
  include Sequel::SQL::OrderMethods
  include Sequel::SQL::BooleanMethods
  include Sequel::SQL::NumericMethods
  include Sequel::SQL::QualifyingMethods
  include Sequel::SQL::StringMethods
  include Sequel::SQL::SubscriptMethods
  include Sequel::SQL::ComplexExpressionMethods

  # Returns receiver wrapped in an <tt>Sequel::SQL::Identifier</tt>.  Usually used to
  # prevent splitting the symbol.
  #
  #   :a__b # SQL: "a"."b"
  #   :a__b.identifier # SQL: "a__b"
  def identifier
    Sequel::SQL::Identifier.new(self)
  end

  # Returns a <tt>Sequel::SQL::Function</tt> with this as the function name,
  # and the given arguments. This is aliased as <tt>Symbol#[]</tt> if the RUBY_VERSION
  # is less than 1.9.0. Ruby 1.9 defines <tt>Symbol#[]</tt>, and Sequel
  # doesn't override methods defined by ruby itself.
  #
  #   :now.sql_function # SQL: now()
  #   :sum.sql_function(:a) # SQL: sum(a)
  #   :concat.sql_function(:a, :b) # SQL: concat(a, b)
  def sql_function(*args)
    Sequel::SQL::Function.new(self, *args)
  end
end
