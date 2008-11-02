module Sequel
  # The SQL module holds classes whose instances represent SQL fragments.
  # It also holds modules that are included in core ruby classes that
  # make Sequel a friendly DSL.
  module SQL
    ### Parent Classes ###

    # Classes/Modules aren't an alphabetical order due to the fact that
    # some reference constants defined in others at load time.

    # Base class for all SQL fragments
    class Expression
      # Returns self, because SQL::Expression already acts like
      # LiteralString.
      def lit
        self
      end
    end

    # Represents a complex SQL expression, with a given operator and one
    # or more attributes (which may also be ComplexExpressions, forming
    # a tree).  This class is the backbone of the blockless filter support in
    # Sequel.
    #
    # This is an abstract class that is not that useful by itself.  The
    # subclasses BooleanExpression, NumericExpression, and StringExpression
    # define the behavior of the DSL via operators.
    class ComplexExpression < Expression
      # A hash of the opposite for each operator symbol, used for inverting
      # objects.
      OPERTATOR_INVERSIONS = {:AND => :OR, :OR => :AND, :< => :>=, :> => :<=,
        :<= => :>, :>= => :<, :'=' => :'!=' , :'!=' => :'=', :LIKE => :'NOT LIKE',
        :'NOT LIKE' => :LIKE, :~ => :'!~', :'!~' => :~, :IN => :'NOT IN',
        :'NOT IN' => :IN, :IS => :'IS NOT', :'IS NOT' => :IS, :'~*' => :'!~*',
        :'!~*' => :'~*', :NOT => :NOOP, :NOOP => :NOT, :ILIKE => :'NOT ILIKE',
        :'NOT ILIKE'=>:ILIKE}

      # Mathematical Operators used in NumericMethods
      MATHEMATICAL_OPERATORS = [:+, :-, :/, :*]

      # Mathematical Operators used in NumericMethods
      BITWISE_OPERATORS = [:&, :|, :^, :<<, :>>]

      # Inequality Operators used in InequalityMethods
      INEQUALITY_OPERATORS = [:<, :>, :<=, :>=]

      # Hash of ruby operator symbols to SQL operators, used in BooleanMethods
      BOOLEAN_OPERATOR_METHODS = {:& => :AND, :| =>:OR}

      # Operator symbols that take exactly two arguments
      TWO_ARITY_OPERATORS = [:'=', :'!=', :IS, :'IS NOT', :LIKE, :'NOT LIKE', \
        :~, :'!~', :'~*', :'!~*', :IN, :'NOT IN', :ILIKE, :'NOT ILIKE'] + \
        INEQUALITY_OPERATORS + BITWISE_OPERATORS 

      # Operator symbols that take one or more arguments
      N_ARITY_OPERATORS = [:AND, :OR, :'||'] + MATHEMATICAL_OPERATORS

      # Operator symbols that take one argument
      ONE_ARITY_OPERATORS = [:NOT, :NOOP, :'B~']

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
        case op
        when *N_ARITY_OPERATORS
          raise(Error, "The #{op} operator requires at least 1 argument") unless args.length >= 1
        when *TWO_ARITY_OPERATORS
          raise(Error, "The #{op} operator requires precisely 2 arguments") unless args.length == 2
        when *ONE_ARITY_OPERATORS
          raise(Error, "The #{op} operator requires a single argument") unless args.length == 1
        else
          raise(Error, "Invalid operator #{op}")
        end
        @op = op
        @args = args
      end
      
      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.complex_expression_sql(@op, @args)
      end

      # Returns true if the receiver is the same expression as the
      # the +other+ expression.
      def eql?( other )
        return other.is_a?( self.class ) &&
          @op.eql?( other.op ) &&
          @args.eql?( other.args )
      end
      alias_method :==, :eql?
    end

    # The base class for expressions that can be used in multiple places in
    # the SQL query.  
    class GenericExpression < Expression
    end
    
    # The base class for expressions that are specific and can only be used
    # in a certain place in the SQL query (ordering, selecting).
    class SpecificExpression < Expression
    end

    ### Modules ###

    # Methods the create aliased identifiers
    module AliasMethods
      # Create an SQL column alias of the receiving column to the given alias.
      def as(aliaz)
        AliasedExpression.new(self, aliaz)
      end
    end

    # This defines the bitwise methods &, |, ^, ~, <<, and >>.  Because these
    # methods overlap with the standard BooleanMethods methods, and they only
    # make sense for numbers, they are only included in NumericExpression.
    module BitwiseMethods
      ComplexExpression::BITWISE_OPERATORS.each do |o|
        define_method(o) do |ce|
          case ce
          when NumericExpression 
            NumericExpression.new(o, self, ce)
          when ComplexExpression
            raise(Sequel::Error, "cannot apply #{o} to a non-numeric expression")
          else  
            NumericExpression.new(o, self, ce)
          end
        end
      end

      # Do the bitwise compliment of the self
      def ~
        NumericExpression.new(:'B~', self)
      end
    end

    # This module includes the methods that are defined on objects that can be 
    # used in a boolean context in SQL (Symbol, LiteralString, SQL::Function,
    # and SQL::BooleanExpression).
    #
    # This defines the ~ (NOT), & (AND), and | (OR) methods.
    module BooleanMethods
      # Create a new BooleanExpression with NOT, representing the inversion of whatever self represents.
      def ~
        BooleanExpression.invert(self)
      end
      
      ComplexExpression::BOOLEAN_OPERATOR_METHODS.each do |m, o|
        define_method(m) do |ce|
          case ce
          when BooleanExpression
            BooleanExpression.new(o, self, ce)
          when ComplexExpression
            raise(Sequel::Error, "cannot apply #{o} to a non-boolean expression")
          else  
            BooleanExpression.new(o, self, ce)
          end
        end
      end
    end

    # Holds methods that are used to cast objects to differen SQL types.
    module CastMethods 
      # Cast the reciever to the given SQL type
      def cast(sql_type)
        IrregularFunction.new(:cast, self, :AS, sql_type.to_s.lit)
      end
      alias_method :cast_as, :cast

      # Cast the reciever to the given SQL type (or integer if none given),
      # and return the result as a NumericExpression. 
      def cast_numeric(sql_type = nil)
        cast(sql_type || :integer).sql_number
      end

      # Cast the reciever to the given SQL type (or text if none given),
      # and return the result as a StringExpression, so you can use +
      # directly on the result for SQL string concatenation.
      def cast_string(sql_type = nil)
        cast(sql_type || :text).sql_string
      end
    end
    
    # Includes a method that returns Identifiers.
    module IdentifierMethods
      # Return self wrapped as an identifier.
      def identifier
        Identifier.new(self)
      end
    end

    # This module includes the methods that are defined on objects that can be 
    # used in a numeric or string context in SQL (Symbol, LiteralString, 
    # SQL::Function, and SQL::StringExpression).
    #
    # This defines the >, <, >=, and <= methods.
    module InequalityMethods
      ComplexExpression::INEQUALITY_OPERATORS.each do |o|
        define_method(o) do |ce|
          case ce
          when BooleanExpression, TrueClass, FalseClass, NilClass, Hash, Array
            raise(Error, "cannot apply #{o} to a boolean expression")
          else  
            BooleanExpression.new(o, self, ce)
          end
        end
      end
    end

    # This module augments the default initalize method for the 
    # ComplexExpression subclass it is included in, so that
    # attempting to use boolean input when initializing a NumericExpression
    # or StringExpression results in an error.
    module NoBooleanInputMethods
      # Raise an Error if one of the args would be boolean in an SQL
      # context, otherwise call super.
      def initialize(op, *args)
        args.each do |a|
          case a
          when BooleanExpression, TrueClass, FalseClass, NilClass, Hash, Array
            raise(Error, "cannot apply #{op} to a boolean expression")
          end
        end
        super
      end
    end

    # This module includes the methods that are defined on objects that can be 
    # used in a numeric context in SQL (Symbol, LiteralString, SQL::Function,
    # and SQL::NumericExpression).
    #
    # This defines the +, -, *, and / methods.
    module NumericMethods
      ComplexExpression::MATHEMATICAL_OPERATORS.each do |o|
        define_method(o) do |ce|
          case ce
          when NumericExpression
            NumericExpression.new(o, self, ce)
          when ComplexExpression
            raise(Sequel::Error, "cannot apply #{o} to a non-numeric expression")
          else  
            NumericExpression.new(o, self, ce)
          end
        end
      end
    end

    # Methods that create OrderedExpressions, used for sorting by columns
    # or more complex expressions.
    module OrderMethods
      # Mark the receiving SQL column as sorting in a descending fashion.
      def desc
        OrderedExpression.new(self)
      end
      
      # Mark the receiving SQL column as sorting in an ascending fashion (generally a no-op).
      def asc
        OrderedExpression.new(self, false)
      end
    end

    # Methods that created QualifiedIdentifiers, used for qualifying column
    # names with a table or table names with a schema.
    module QualifyingMethods
      # Qualify the current object with the given table/schema.
      def qualify(ts)
        QualifiedIdentifier.new(ts, self)
      end
    end

    # This module includes the methods that are defined on objects that can be 
    # used in a numeric context in SQL (Symbol, LiteralString, SQL::Function,
    # and SQL::StringExpression).
    #
    # This defines the like (LIKE) method, used for pattern matching.
    module StringMethods
      # Create a BooleanExpression case insensitive pattern match of self
      # with the given patterns.  See StringExpression.like.
      def ilike(*ces)
        StringExpression.like(self, *(ces << {:case_insensitive=>true}))
      end

      # Create a BooleanExpression case sensitive pattern match of self with
      # the given patterns.  See StringExpression.like.
      def like(*ces)
        StringExpression.like(self, *ces)
      end
    end

    # This module is included in StringExpression and can be included elsewhere
    # to allow the use of the + operator to represent concatenation of SQL
    # Strings:
    #
    #   :x.sql_string + :y => # SQL: x || y
    module StringConcatenationMethods
      def +(ce)
        StringExpression.new(:'||', self, ce)
      end
    end

    ### Modules that include other modules ###

    # This module includes other Sequel::SQL::*Methods modules and is
    # included in other classes that are could be either booleans,
    # strings, or numbers.  It also adds three methods so that
    # can specify behavior in case one of the operator methods has
    # been overridden (such as Symbol#/).
    #
    # For example, if Symbol#/ is overridden to produce a string (for
    # example, to make file system path creation easier), the
    # following code will not do what you want:
    #
    #   :price/10 > 100
    #
    # In that case, you need to do the following:
    #
    #   :price.sql_number/10 > 100
    module ComplexExpressionMethods
      include BooleanMethods
      include NumericMethods
      include StringMethods
      include InequalityMethods

      # Extract a datetime_part (e.g. year, month) from self:
      #
      #   :date.extract(:year) # SQL:  extract(year FROM date)
      #
      # Also has the benefit of returning the result as a
      # NumericExpression instead of a generic ComplexExpression.
      def extract(datetime_part)
        IrregularFunction.new(:extract, datetime_part.to_s.lit, :FROM, self).sql_number
      end

      # Return a BooleanExpression representation of self.
      def sql_boolean
        BooleanExpression.new(:NOOP, self)
      end

      # Return a NumericExpression representation of self.
      def sql_number
        NumericExpression.new(:NOOP, self)
      end

      # Return a StringExpression representation of self.
      def sql_string
        StringExpression.new(:NOOP, self)
      end
    end

    module SpecificExpressionMethods
      include AliasMethods
      include CastMethods
      include OrderMethods
    end

    module GenericExpressionMethods
      include SpecificExpressionMethods
      include ComplexExpressionMethods
    end

    class ComplexExpression
      include SpecificExpressionMethods
    end

    class GenericExpression
      include GenericExpressionMethods
    end

    ### Classes ###

    # Represents an aliasing of an expression/column to a given name.
    class AliasedExpression < SpecificExpression
      # The expression to alias
      attr_reader :expression

      # The alias to use for the expression, not alias since that is
      # a keyword in ruby.
      attr_reader :aliaz

      # Create an object with the given expression and alias.
      def initialize(expression, aliaz)
        @expression, @aliaz = expression, aliaz
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.aliased_expression_sql(self)
      end
    end

    # Represents an SQL array.  Added so it is possible to deal with a
    # ruby array of all two pairs as an SQL array instead of an ordered
    # hash-like conditions specifier.
    class SQLArray < Expression
      # Create an object with the given array.
      def initialize(array)
        @array = array
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.array_sql(@array)
      end
    end

    # Blob is used to represent binary data in the Ruby environment that is
    # stored as a blob type in the database. In PostgreSQL, the blob type is 
    # called bytea. Sequel represents binary data as a Blob object because 
    # certain database engines, such as PostgreSQL, require binary data to be 
    # escaped.
    class Blob < ::String
      # return self.
      def to_blob
        self
      end
    end

    # Subclass of ComplexExpression where the expression results
    # in a boolean value in SQL.
    class BooleanExpression < ComplexExpression
      include BooleanMethods
      
      # Take pairs of values (e.g. a hash or array of arrays of two pairs)
      # and converts it to a BooleanExpression.  The operator and args
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
          when Array, ::Sequel::Dataset, SQLArray
            new(:IN, l, r)
          when NilClass
            new(:IS, l, r)
          when Regexp
            StringExpression.like(l, r)
          else
            new(:'=', l, r)
          end
          negate ? invert(ce) : ce
        end
        pairs.length == 1 ? pairs.at(0) : new(op, *pairs)
      end
      
      # Invert the expression, if possible.  If the expression cannot
      # be inverted, raise an error.  An inverted expression should match everything that the
      # uninverted expression did not match, and vice-versa.
      def self.invert(ce)
        case ce
        when BooleanExpression
          case op = ce.op
          when :AND, :OR
            BooleanExpression.new(OPERTATOR_INVERSIONS[op], *ce.args.collect{|a| BooleanExpression.invert(a)})
          else
            BooleanExpression.new(OPERTATOR_INVERSIONS[op], *ce.args.dup)
          end
        when ComplexExpression
          raise(Sequel::Error, "operator #{ce.op} cannot be inverted")
        else
          BooleanExpression.new(:NOT, ce)
        end
      end
    end

    # Represents an SQL CASE expression, used for conditions.
    class CaseExpression < GenericExpression
      # An array of all two pairs with the first element specifying the
      # condition and the second element specifying the result.
      attr_reader :conditions

      # The default value if no conditions are true
      attr_reader :default

      # The expression to test the conditions against
      attr_reader :expression

      # Create an object with the given conditions and
      # default value.
      def initialize(conditions, default, expression = nil)
        raise(Sequel::Error, 'CaseExpression conditions must be an array with all_two_pairs') unless Array === conditions and conditions.all_two_pairs?
        @conditions, @default, @expression = conditions, default, expression
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.case_expression_sql(self)
      end
    end

    # Represents all columns in a given table, table.* in SQL
    class ColumnAll < SpecificExpression
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

    # Represents an SQL function call.
    class Function < GenericExpression
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
    
    # Represents an identifier (column or table). Can be used
    # to specify a Symbol with multiple underscores should not be
    # split, or for creating an identifier without using a symbol.
    class Identifier < GenericExpression
      include QualifyingMethods

      # The table and column to reference
      attr_reader :value

      # Set the value to the given argument
      def initialize(value)
        @value = value
      end
      
      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.quote_identifier(@value)
      end 
    end
    
    # IrregularFunction is used for the SQL EXTRACT and CAST functions,
    # which don't use regular function calling syntax. The IrregularFunction
    # replaces the commas the regular function uses with a custom
    # join string.
    #
    # This shouldn't be used directly, see CastMethods#cast and 
    # ComplexExpressionMethods#extract.
    class IrregularFunction < Function
      # The arguments to pass to the function (may be blank)
      attr_reader :arg1, :arg2

      # The SQL function to call
      attr_reader :f
      
      # The literal string to use in place of a comma to join arguments
      attr_reader :joiner

      # Set the attributes to the given arguments
      def initialize(f, arg1, joiner, arg2)
        @f, @arg1, @joiner, @arg2 = f, arg1, joiner, arg2
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.irregular_function_sql(self)
      end
    end

    # Represents an SQL JOIN clause, used for joining tables.
    class JoinClause < SpecificExpression
      # The type of join to do
      attr_reader :join_type

      # The actual table to join
      attr_reader :table

      # The table alias to use for the join, if any
      attr_reader :table_alias

      # Create an object with the given conditions and
      # default value.
      def initialize(join_type, table, table_alias = nil)
        @join_type, @table, @table_alias = join_type, table, table_alias
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.join_clause_sql(self)
      end
    end

    # Represents an SQL JOIN table ON conditions clause.
    class JoinOnClause < JoinClause
      # The conditions for the join
      attr_reader :on

      # Create an object with the given conditions and
      # default value.
      def initialize(on, *args)
        @on = on
        super(*args)
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.join_on_clause_sql(self)
      end
    end

    # Represents an SQL JOIN table USING (columns) clause.
    class JoinUsingClause < JoinClause
      # The columns that appear both tables that should be equal 
      # for the conditions to match.
      attr_reader :using

      # Create an object with the given conditions and
      # default value.
      def initialize(using, *args)
        @using = using
        super(*args)
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.join_using_clause_sql(self)
      end
    end

    # Subclass of ComplexExpression where the expression results
    # in a numeric value in SQL.
    class NumericExpression < ComplexExpression
      include BitwiseMethods 
      include NumericMethods
      include InequalityMethods
      include NoBooleanInputMethods
    end

    # Represents a column/expression to order the result set by.
    class OrderedExpression < SpecificExpression
      # The expression to order the result set by.
      attr_reader :expression

      # Whether the expression should order the result set in a descending manner
      attr_reader :descending

      # Set the expression and descending attributes to the given values.
      def initialize(expression, descending = true)
        @expression, @descending = expression, descending
      end

      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.ordered_expression_sql(self)
      end
    end

    # Represents a qualified (column with table) reference.  Used when
    # joining tables to disambiguate columns.
    class QualifiedIdentifier < GenericExpression
      # The table and column to reference
      attr_reader :table, :column

      # Set the attributes to the given arguments
      def initialize(table, column)
        @table, @column = table, column
      end
      
      # Delegate the creation of the resulting SQL to the given dataset,
      # since it may be database dependent.
      def to_s(ds)
        ds.qualified_identifier_sql(self)
      end 
    end
    
    # Subclass of ComplexExpression where the expression results
    # in a text/string/varchar value in SQL.
    class StringExpression < ComplexExpression
      include StringMethods
      include StringConcatenationMethods
      include InequalityMethods
      include NoBooleanInputMethods
      
      # Creates a SQL pattern match exprssion. left (l) is the SQL string we
      # are matching against, and ces are the patterns we are matching.
      # The match succeeds if any of the patterns match (SQL OR).  Patterns
      # can be given as strings or regular expressions.  Strings will cause
      # the SQL LIKE operator to be used, and should be supported by most
      # databases.  Regular expressions will probably only work on MySQL
      # and PostgreSQL, and SQL regular expression syntax is not fully compatible
      # with ruby regular expression syntax, so be careful if using regular
      # expressions.
      # 
      # The pattern match will be case insensitive if the last argument is a hash
      # with a key of :case_insensitive that is not false or nil. Also,
      # if a case insensitive regular expression is used (//i), that particular
      # pattern which will always be case insensitive.
      def self.like(l, *ces)
        case_insensitive = ces.extract_options![:case_insensitive]
        ces.collect! do |ce|
          op, expr = Regexp === ce ? [ce.casefold? || case_insensitive ? :'~*' : :~, ce.source] : [case_insensitive ? :ILIKE : :LIKE, ce]
          BooleanExpression.new(op, l, expr)
        end
        ces.length == 1 ? ces.at(0) : BooleanExpression.new(:OR, *ces)
      end
    end

    # Represents an SQL array access, with multiple possible arguments.
    class Subscript < GenericExpression
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

    if RUBY_VERSION >= '1.9.0'
      class VirtualRow < BasicObject
      end
    else
      class VirtualRow
        (instance_methods - %w"__id__ __send__ instance_eval == equal?").each{|m| undef_method(m)}
      end
    end

    # An instance of this class is yielded to the block supplied to filter.
    # Useful if another library also defines the operator methods that
    # Sequel defines for symbols.
    #
    # Examples:
    #
    #   ds = DB[:t]
    #   ds.filter{|r| r.name < 2} # SELECT * FROM t WHERE (name < 2)
    #   ds.filter{|r| r.table__column + 1 < 2} # SELECT * FROM t WHERE ((table.column + 1) < 2)
    #   ds.filter{|r| r.is_active(1, 'arg2')} # SELECT * FROM t WHERE is_active(1, 'arg2')
    class VirtualRow
      # Can return Identifiers, QualifiedIdentifiers, or Functions:
      #
      # * Function - returned if any arguments are supplied, using the method name
      #   as the function name, and the arguments as the function arguments.
      # * QualifiedIdentifier - returned if the method name contains __, with the
      #   table being the part before __, and the column being the part after.
      # * Identifier - returned otherwise, using the method name.
      def method_missing(m, *args)
        if args.empty?
          table, column = m.to_s.split('__', 2)
          column ? QualifiedIdentifier.new(table, column) : Identifier.new(m)
        else
          Function.new(m, *args)
        end
      end
    end
  end

  # LiteralString is used to represent literal SQL expressions. A 
  # LiteralString is copied verbatim into an SQL statement. Instances of
  # LiteralString can be created by calling String#lit.
  # LiteralStrings can use all of the SQL::ColumnMethods and the 
  # SQL::ComplexExpressionMethods.
  class LiteralString < ::String
    include SQL::OrderMethods
    include SQL::ComplexExpressionMethods
  end
end
