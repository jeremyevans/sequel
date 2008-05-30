# This file holds classes and modules under Sequel that are related to SQL
# creation.

module Sequel
  # The SQL module holds classes whose instances represent SQL fragments.
  # It also holds modules that are included in core ruby classes that
  # make Sequel a friendly DSL.
  module SQL
    
    ### Classes ###

    # Base class for all SQL fragments
    class Expression
      # Returns self, because SQL::Expression already acts like
      # LiteralString.
      def lit
        self
      end
    end
    
    # Represents all columns in a given table, table.* in SQL
    class ColumnAll < Expression
      # The table containing the columns being selected
      attr_reader :table

      # Create an object with the given table
      def initialize(table)
        @table = table
      end

      # ColumnAll expressions are considered equivalent if they
      # have the same class and string representation
      def ==(x)
        x.class == self.class && @table == x.table
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.column_all_sql(self)
      end
    end

    # Represents a generic column expression, used for specifying order
    # and aliasing of columns.
    class ColumnExpr < Expression
      # Created readers for the left, operator, and right expression.
      attr_reader :l, :op, :r

      # Sets the attributes for the object to those given.
      # The right (r) is not required, it is used when aliasing.
      # The left (l) usually specifies the column name, and the
      # operator (op) is usually 'ASC', 'DESC', or 'AS'.
      def initialize(l, op, r = nil)
        @l, @op, @r = l, op, r
      end
      
      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.column_expr_sql(self)
      end
    end
    
    # Represents a complex SQL expression, with a given operator and one
    # or more attributes (which may also be ComplexExpressions, forming
    # a tree).  This class is the backbone of the blockless filter support in
    # Sequel.
    #
    # Most ruby operators methods are defined via metaprogramming: +, -, /, *, <, >, <=,
    # >=, & (AND), | (OR). This allows for a simple DSL after some core
    # classes have been overloaded with ComplexExpressionMethods.
    class ComplexExpression < Expression
      # A hash of the opposite for each operator symbol, used for inverting 
      # objects.
      OPERTATOR_INVERSIONS = {:AND => :OR, :OR => :AND, :< => :>=, :> => :<=,
        :<= => :>, :>= => :<, :'=' => :'!=' , :'!=' => :'=', :LIKE => :'NOT LIKE',
        :'NOT LIKE' => :LIKE, :~ => :'!~', :'!~' => :~, :IN => :'NOT IN',
        :'NOT IN' => :IN, :IS => :'IS NOT', :'IS NOT' => :IS, :'~*' => :'!~*',
        :'!~*' => :'~*'}

      MATHEMATICAL_OPERATORS = [:+, :-, :/, :*]
      INEQUALITY_OPERATORS = [:<, :>, :<=, :>=]
      STRING_OPERATORS = [:'||']
      SEARCH_OPERATORS = [:LIKE, :'NOT LIKE', :~, :'!~', :'~*', :'!~*']
      INCLUSION_OPERATORS = [:IN, :'NOT IN']
      BOOLEAN_OPERATORS = [:AND, :OR]

      # Collection of all equality/inequality operator symbols
      EQUALITY_OPERATORS = [:'=', :'!=', :IS, :'IS NOT', *INEQUALITY_OPERATORS]

      # Operator symbols that do not work on boolean SQL input
      NO_BOOLEAN_INPUT_OPERATORS = MATHEMATICAL_OPERATORS + INEQUALITY_OPERATORS + STRING_OPERATORS

      # Operator symbols that result in boolean SQL output
      BOOLEAN_RESULT_OPERATORS = BOOLEAN_OPERATORS + EQUALITY_OPERATORS + SEARCH_OPERATORS + INCLUSION_OPERATORS + [:NOT]

      # Literal SQL booleans that are not allowed
      BOOLEAN_LITERALS = [true, false, nil]

      # Hash of ruby operator symbols to SQL operators, used for method creation
      BOOLEAN_OPERATOR_METHODS = {:& => :AND, :| =>:OR}

      # Operator symbols that take exactly two arguments
      TWO_ARITY_OPERATORS = EQUALITY_OPERATORS + SEARCH_OPERATORS + INCLUSION_OPERATORS

      # Operator symbols that take one or more arguments
      N_ARITY_OPERATORS = MATHEMATICAL_OPERATORS + BOOLEAN_OPERATORS + STRING_OPERATORS

      # An array of args for this object
      attr_reader :args

      # The operator symbol for this object
      attr_reader :op

      # Set the operator symbol and arguments for this object to the ones given.
      # Convert all args that are hashes or arrays with all two pairs to ComplexExpressions.
      # Raise an error if the operator doesn't allow boolean input and a boolean argument is given.
      # Raise an error if the wrong number of arguments for a given operator is used.
      def initialize(op, *args)
        args.collect! do |a|
          case a
          when Hash
            a.sql_expr
          when Array
            a.all_two_pairs? ? a.sql_expr : a
          else
            a
          end
        end
        if NO_BOOLEAN_INPUT_OPERATORS.include?(op)
          args.each do |a|
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

      # Take pairs of values (e.g. a hash or array of arrays of two pairs)
      # and converts it to a ComplexExpression.  The operator and args
      # used depends on the case of the right (2nd) argument:
      # 
      # * 0..10 - left >= 0 AND left <= 10
      # * [1,2] - left IN (1,2)
      # * nil - left IS NULL
      # * /as/ - left ~ 'as'
      # * :blah - left = blah
      # * 'blah' - left = 'blah'
      #
      # If multiple arguments are given, they are joined with the op given (AND
      # by default, OR possible).  If negate is set to true,
      # all subexpressions are inverted before used.  Therefore, the following
      # expressions are equivalent:
      #
      #   ~from_value_pairs(hash)
      #   from_value_pairs(hash, :OR, true)
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

      # Creates a SQL pattern match exprssion. left (l) is the SQL string we
      # are matching against, and ces are the patterns we are matching.
      # The match succeeds if any of the patterns match (SQL OR).  Patterns
      # can be given as strings or regular expressions.  Strings will cause
      # the SQL LIKE operator to be used, and should be supported by most
      # databases.  Regular expressions will probably only work on MySQL
      # and PostgreSQL, and SQL regular expression syntax is not fully compatible
      # with ruby regular expression syntax, so be careful if using regular 
      # expressions.
      def self.like(l, *ces)
        ces.collect! do |ce| 
          op, expr = Regexp === ce ? [ce.casefold? ? :'~*' : :~, ce.source] : [:LIKE, ce.to_s]
          new(op, l, expr)
        end
        ces.length == 1 ? ces.at(0) : new(:OR, *ces)
      end

      # Invert the regular expression, if possible.  If the expression cannot
      # be inverted, raise an error.  An inverted expression should match everything that the
      # uninverted expression did not match, and vice-versa.
      def ~
        case @op
        when *BOOLEAN_OPERATORS
          self.class.new(OPERTATOR_INVERSIONS[@op], *@args.collect{|a| ComplexExpression === a ? ~a : ComplexExpression.new(:NOT, a)})
        when *TWO_ARITY_OPERATORS
          self.class.new(OPERTATOR_INVERSIONS[@op], *@args.dup)
        when :NOT
          @args.first
        else
          raise(Sequel::Error, "operator #{@op} cannot be inverted")
        end
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.complex_expression_sql(@op, @args)
      end

      BOOLEAN_OPERATOR_METHODS.each do |m, o|
        define_method(m) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a non-boolean expression") unless BOOLEAN_RESULT_OPERATORS.include?(@op)
          super
        end
      end

      (MATHEMATICAL_OPERATORS + INEQUALITY_OPERATORS).each do |o|
        define_method(o) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a boolean expression") unless NO_BOOLEAN_INPUT_OPERATORS.include?(@op)
          super
        end
      end
    end

    # Represents an SQL function call.
    class Function < Expression
      # The array of arguments to pass to the function (may be blank)
      attr_reader :args

      # The SQL function to call
      attr_reader :f
      
      # Set the attributes to the given arguments
      def initialize(f, *args)
        @f, @args = f, args
      end

      # Functions are considered equivalent if they
      # have the same class, function, and arguments.
      def ==(x)
         x.class == self.class && @f == x.f && @args == x.args
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.function_sql(self)
      end
    end
    
    # Represents a qualified (column with table) reference.  Used when
    # joining tables to disambiguate columns.
    class QualifiedColumnRef < Expression
      # The table and column to reference
      attr_reader :table, :column

      # Set the attributes to the given arguments
      def initialize(table, column)
        @table, @column = table, column
      end
      
      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.qualified_column_ref_sql(self)
      end 
    end
    
    # Represents an SQL array access, with multiple possible arguments.
    class Subscript < Expression
      # The SQL array column
      attr_reader :f

      # The array of subscripts to use (should be an array of numbers)
      attr_reader :sub

      # Set the attributes to the given arguments
      def initialize(f, sub)
        @f, @sub = f, sub
      end

      # Create a new subscript appending the given subscript(s)
      # the the current array of subscripts.
      def |(sub)
        Subscript.new(@f, @sub + Array(sub))
      end
      
      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.subscript_sql(self)
      end
    end

    ### Modules ###
    
    # Module included in core classes giving them a simple and easy DSL
    # for creation of ComplexExpressions.
    #
    # Most ruby operators methods are defined via metaprogramming: +, -, /, *, <, >, <=,
    # >=, & (AND), | (OR). 
    module ComplexExpressionMethods
      NO_BOOLEAN_INPUT_OPERATORS = ComplexExpression::NO_BOOLEAN_INPUT_OPERATORS
      BOOLEAN_RESULT_OPERATORS = ComplexExpression::BOOLEAN_RESULT_OPERATORS
      BOOLEAN_OPERATOR_METHODS = ComplexExpression::BOOLEAN_OPERATOR_METHODS

      BOOLEAN_OPERATOR_METHODS.each do |m, o|
        define_method(m) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a non-boolean expression") if (ComplexExpression === ce) && !BOOLEAN_RESULT_OPERATORS.include?(ce.op)
          ComplexExpression.new(o, self, ce)   
        end
      end

      (ComplexExpression::MATHEMATICAL_OPERATORS + ComplexExpression::INEQUALITY_OPERATORS).each do |o|
        define_method(o) do |ce|
          raise(Sequel::Error, "cannot apply #{o} to a boolean expression") if (ComplexExpression === ce) && !NO_BOOLEAN_INPUT_OPERATORS.include?(ce.op)
          ComplexExpression.new(o, self, ce)   
        end
      end

      # Create a new ComplexExpression with NOT, representing the inversion of whatever self represents.
      def ~
        ComplexExpression.new(:NOT, self)
      end

      # Create a ComplexExpression pattern match of self with the given patterns.
      def like(*ces)
        ComplexExpression.like(self, *ces)
      end
    end

    # Holds methods that should be called on columns only.
    module ColumnMethods
      AS = 'AS'.freeze
      DESC = 'DESC'.freeze
      ASC = 'ASC'.freeze
      
      # Create an SQL column alias of the receiving column to the given alias.
      def as(a)
        ColumnExpr.new(self, AS, a)
      end
      
      # Mark the receiving SQL column as sorting in a descending fashion.
      def desc
        ColumnExpr.new(self, DESC)
      end
      
      # Mark the receiving SQL column as sorting in an ascending fashion (generally a no-op).
      def asc
        ColumnExpr.new(self, ASC)
      end

      # Cast the reciever to the given SQL type
      def cast_as(t)
        t = t.to_s.lit if t.is_a?(Symbol)
        Sequel::SQL::Function.new(:cast, self.as(t))
      end
    end

    class Expression
      # Include the modules in Expression, couldn't be done
      # earlier due to cyclic dependencies.
      include ColumnMethods
      include ComplexExpressionMethods
    end
  end

  # LiteralString is used to represent literal SQL expressions. An 
  # LiteralString is copied verbatim into an SQL statement. Instances of
  # LiteralString can be created by calling String#lit.
  # LiteralStrings can use all of the SQL::ColumnMethods and the 
  # SQL::ComplexExpressionMethods.
  class LiteralString < ::String
    include SQL::ComplexExpressionMethods
  end
end
