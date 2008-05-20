class Array
  def ~
    to_complex_expr_if_all_two_pairs(:OR, true)
  end

  def all_two_pairs?
    !empty? && all?{|i| (Array === i) && (i.length == 2)}
  end

  # Concatenates an array of strings into an SQL string. ANSI SQL and C-style
  # comments are removed, as well as excessive white-space.
  def to_sql
    map {|l| ((m = /^(.*)--/.match(l)) ? m[1] : l).chomp}.join(' '). \
      gsub(/\/\*.*\*\//, '').gsub(/\s+/, ' ').strip
  end

  def sql_negate
    to_complex_expr_if_all_two_pairs(:AND, true)
  end

  def sql_or
    to_complex_expr_if_all_two_pairs(:OR)
  end

  def to_complex_expr
    to_complex_expr_if_all_two_pairs
  end

  private

  def to_complex_expr_if_all_two_pairs(*args)
    raise(Sequel::Error, 'Not all elements of the array are arrays of size 2, so it cannot be converted to an SQL expression') unless all_two_pairs?
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, *args)
  end
end

module Sequel
  # LiteralString is used to represent literal SQL expressions. An 
  # LiteralString is copied verbatim into an SQL statement. Instances of
  # LiteralString can be created by calling String#lit.
  class LiteralString < ::String
  end
end

class String
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
  
  # Converts a string into a Date object.
  def to_date
    begin
      Date.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid date value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a DateTime object.
  def to_datetime
    begin
      DateTime.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid date value '#{self}' (#{e.message})"
    end
  end

  # Converts a string into a Time object.
  def to_time
    begin
      Time.parse(self)
    rescue => e
      raise Sequel::Error::InvalidValue, "Invalid time value '#{self}' (#{e.message})"
    end
  end
end


module Sequel
  module SQL
    module ColumnMethods
      AS = 'AS'.freeze
      DESC = 'DESC'.freeze
      ASC = 'ASC'.freeze
      
      def as(a); ColumnExpr.new(self, AS, a); end
      
      def desc; ColumnExpr.new(self, DESC); end
      
      def asc; ColumnExpr.new(self, ASC); end

      def cast_as(t)
        t = t.to_s.lit if t.is_a?(Symbol)
        Sequel::SQL::Function.new(:cast, self.as(t))
      end
    end

    class Expression
      include ColumnMethods

      def lit; self; end
    end
    
    class ColumnExpr < Expression
      attr_reader :l, :op, :r

      def initialize(l, op, r = nil)
        @l, @op, @r = l, op, r
      end
      
      def to_s(ds)
        ds.column_expr_sql(self)
      end
    end
    
    class QualifiedColumnRef < Expression
      attr_reader :table, :column

      def initialize(table, column)
        @table, @column = table, column
      end
      
      def to_s(ds)
        ds.qualified_column_ref_sql(self)
      end 
    end
    
    class Function < Expression
      attr_reader :f, :args

      def initialize(f, *args)
        @f, @args = f, args
      end

      # Functions are considered equivalent if they
      # have the same class, function, and arguments.
      def ==(x)
         x.class == self.class && @f == x.f && @args == x.args
      end

      def to_s(ds)
        ds.function_sql(self)
      end
    end
    
    class Subscript < Expression
      attr_reader :f, :sub

      def initialize(f, sub)
        @f, @sub = f, sub
      end

      def |(sub)
        Subscript.new(@f, @sub << Array(sub))
      end
      
      def to_s(ds)
        ds.subscript_sql(self)
      end
    end
    
    class ColumnAll < Expression
      attr_reader :table

      def initialize(table)
        @table = table
      end

      # ColumnAll expressions are considered equivalent if they
      # have the same class and string representation
      def ==(x)
        x.class == self.class && @table == x.table
      end

      def to_s(ds)
        ds.column_all_sql(self)
      end
    end

    class ComplexExpression < Expression
      OPERTATOR_INVERSIONS = {:AND => :OR, :OR => :AND, :< => :>=, :> => :<=,
        :<= => :>, :>= => :<, :'=' => :'!=' , :'!=' => :'=', :LIKE => :'NOT LIKE',
        :'NOT LIKE' => :LIKE, :~ => :'!~', :'!~' => :~, :IN => :'NOT IN',
        :'NOT IN' => :IN, :IS => :'IS NOT', :'IS NOT' => :IS, :'~*' => :'!~*',
        :'!~*' => :'~*'}

      MATHEMATICAL_OPERATORS = [:+, :-, :/, :*]
      INEQUALITY_OPERATORS = [:<, :>, :<=, :>=]
      SEARCH_OPERATORS = [:LIKE, :'NOT LIKE', :~, :'!~', :'~*', :'!~*']
      INCLUSION_OPERATORS = [:IN, :'NOT IN']
      BOOLEAN_OPERATORS = [:AND, :OR]
      BOOLEAN_OPERATOR_METHODS = {:& => :AND, :| =>:OR}

      EQUALITY_OPERATORS = [:'=', :'!=', :IS, :'IS NOT', *INEQUALITY_OPERATORS]
      NO_BOOLEAN_INPUT_OPERATORS = MATHEMATICAL_OPERATORS + INEQUALITY_OPERATORS
      BOOLEAN_RESULT_OPERATORS = BOOLEAN_OPERATORS + EQUALITY_OPERATORS + SEARCH_OPERATORS + INCLUSION_OPERATORS + [:NOT]
      BOOLEAN_LITERALS = [true, false, nil]

      TWO_ARITY_OPERATORS = EQUALITY_OPERATORS + SEARCH_OPERATORS + INCLUSION_OPERATORS
      N_ARITY_OPERATORS = MATHEMATICAL_OPERATORS + BOOLEAN_OPERATORS

      attr_reader :op, :args

      def initialize(op, *args)
        args.collect! do |a|
          case a
          when Hash
            a.to_complex_expr
          when Array
            a.all_two_pairs? ? a.to_complex_expr : a
          else
            a
          end
        end
        if NO_BOOLEAN_INPUT_OPERATORS.include?(op)
          args.any? do |a|
            if BOOLEAN_LITERALS.include?(a) || ((ComplexExpression === a) && BOOLEAN_RESULT_OPERATORS.include?(a.op))
              raise(Sequel::Error, "cannot apply #{op} to a boolean expression")
            end
          end
        end
        case op
        when *N_ARITY_OPERATORS
          raise(Sequel::Error, 'mathematical and boolean operators require at least 1 argument') unless args.length >= 1
        when *TWO_ARITY_OPERATORS
          raise(Sequel::Error, '(in)equality operators require precisely 2 arguments') unless args.length == 2
        when :NOT
          raise(Sequel::Error, 'the NOT operator requires a single argument') unless args.length == 1
        else
          raise(Sequel::Error, "invalid operator #{op}")
        end
        @op = op
        @args = args
      end

      def self.from_value_pairs(pairs, op=:AND, negate=false)
        pairs = pairs.collect do |l,r|
          ce = case r
          when Range
            new(:AND, new(:>=, l, r.begin), new(r.exclude_end? ? :< : :<=, l, r.end))
          when Array, ::Sequel::Dataset
            new(:IN, l, r)
          when NilClass
            new(:IS, l, r)
          when Regexp
            like(l, r)
          else
            new(:'=', l, r)
          end
          negate ? ~ce : ce
        end
        pairs.length == 1 ? pairs.at(0) : new(op, *pairs)
      end

      def self.like(l, *ces)
        ces.collect! do |ce| 
          op, expr = Regexp === ce ? [ce.casefold? ? :'~*' : :~, ce.source] : [:LIKE, ce.to_s]
          new(op, l, expr)
        end
        ces.length == 1 ? ces.at(0) : new(:OR, *ces)
      end

      def ~
        case op
        when *MATHEMATICAL_OPERATORS
          raise(Sequel::Error, 'mathematical operators cannot be inverted')
        when *BOOLEAN_OPERATORS
          self.class.new(OPERTATOR_INVERSIONS[@op], *@args.collect{|a| ComplexExpression === a ? ~a : ComplexExpression.new(:NOT, a)})
        when *TWO_ARITY_OPERATORS
          self.class.new(OPERTATOR_INVERSIONS[@op], *@args.dup)
        when :NOT
          @args.first
        else
          raise(Sequel::Error, "invalid operator #{op}")
        end
      end

      BOOLEAN_OPERATOR_METHODS.each do |m, o|
        define_method(m) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a non-boolean expression") unless BOOLEAN_RESULT_OPERATORS.include?(op)
          super
        end
      end

      NO_BOOLEAN_INPUT_OPERATORS.each do |o|
        define_method(o) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a boolean expression") unless NO_BOOLEAN_INPUT_OPERATORS.include?(op)
          super
        end
      end

      def to_s(ds)
        ds.complex_expression_sql(@op, @args)
      end
    end

    module ComplexExpressionMethods
      NO_BOOLEAN_INPUT_OPERATORS = ::Sequel::SQL::ComplexExpression::NO_BOOLEAN_INPUT_OPERATORS
      BOOLEAN_RESULT_OPERATORS = ::Sequel::SQL::ComplexExpression::BOOLEAN_RESULT_OPERATORS
      BOOLEAN_OPERATOR_METHODS = ::Sequel::SQL::ComplexExpression::BOOLEAN_OPERATOR_METHODS

      BOOLEAN_OPERATOR_METHODS.each do |m, o|
        define_method(m) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a non-boolean expression") if (ComplexExpression === ce) && !BOOLEAN_RESULT_OPERATORS.include?(ce.op)
          ::Sequel::SQL::ComplexExpression.new(o, self, ce)   
        end
      end

      NO_BOOLEAN_INPUT_OPERATORS.each do |o|
        define_method(o) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a boolean expression") if (ComplexExpression === ce) && !NO_BOOLEAN_INPUT_OPERATORS.include?(ce.op)
          ::Sequel::SQL::ComplexExpression.new(o, self, ce)   
        end
      end

      def ~
        ::Sequel::SQL::ComplexExpression.new(:NOT, self)
      end

      def like(*ces)
        ::Sequel::SQL::ComplexExpression.like(self, *ces)
      end
    end
  end
end

class String
  include Sequel::SQL::ColumnMethods
end

module Sequel
  class LiteralString
    include SQL::ComplexExpressionMethods
  end
  module SQL
    class Expression
      include ComplexExpressionMethods
    end
  end
end

class Symbol
  include Sequel::SQL::ColumnMethods
  include Sequel::SQL::ComplexExpressionMethods

  def *(ce=(arg=false;nil))
    return super(ce) unless arg == false
    Sequel::SQL::ColumnAll.new(self);
  end

  def [](*args)
    Sequel::SQL::Function.new(self, *args)
  end

  def |(sub)
    return super unless (Integer === sub) || ((Array === sub) && sub.any?{|x| Integer === x})
    Sequel::SQL::Subscript.new(self, Array(sub))
  end
  
  def to_column_ref(ds)
    ds.symbol_to_column_ref(self)
  end
end

class Hash
  def &(ce)
    ::Sequel::SQL::ComplexExpression.new(:AND, self, ce)
  end

  def |(ce)
    ::Sequel::SQL::ComplexExpression.new(:OR, self, ce)
  end

  def ~
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, :OR, true)
  end

  def sql_negate
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, :AND, true)
  end

  def sql_or
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self, :OR)
  end

  def to_complex_expr
    ::Sequel::SQL::ComplexExpression.from_value_pairs(self)
  end
end
