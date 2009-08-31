module Sequel
  if RUBY_VERSION < '1.9.0'
    class BasicObject
      (instance_methods - %w"__id__ __send__ instance_eval == equal?").each{|m| undef_method(m)}
    end
  end

  class LiteralString < ::String
  end

  # The SQL module holds classes whose instances represent SQL fragments.
  # It also holds modules that are included in core ruby classes that
  # make Sequel a friendly DSL.
  module SQL

    ### Parent Classes ###

    # Classes/Modules aren't an alphabetical order due to the fact that
    # some reference constants defined in others at load time.

    # Base class for all SQL fragments
    class Expression
      # Create a to_s instance method that takes a dataset, and calls
      # the method provided on the dataset with args as the argument (self by default).
      # Used to DRY up some code.
      def self.to_s_method(meth, args=:self) # :nodoc:
        class_eval("def to_s(ds); ds.#{meth}(#{args}) end", __FILE__, __LINE__)
      end
      private_class_method :to_s_method

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

      # Operators that use IS, used for special casing to override literal true/false values
      IS_OPERATORS = [:IS, :'IS NOT']

      # Operator symbols that take exactly two arguments
      TWO_ARITY_OPERATORS = [:'=', :'!=', :LIKE, :'NOT LIKE', \
        :~, :'!~', :'~*', :'!~*', :IN, :'NOT IN', :ILIKE, :'NOT ILIKE'] + \
        INEQUALITY_OPERATORS + BITWISE_OPERATORS + IS_OPERATORS 

      # Operator symbols that take one or more arguments
      N_ARITY_OPERATORS = [:AND, :OR, :'||'] + MATHEMATICAL_OPERATORS

      # Operator symbols that take one argument
      ONE_ARITY_OPERATORS = [:NOT, :NOOP, :'B~']

      # An array of args for this object
      attr_reader :args

      # The operator symbol for this object
      attr_reader :op
      
      # Set the operator symbol and arguments for this object to the ones given.
      # Convert all args that are hashes or arrays with all two pairs to BooleanExpressions.
      # Raise an error if the operator doesn't allow boolean input and a boolean argument is given.
      # Raise an error if the wrong number of arguments for a given operator is used.
      def initialize(op, *args)
        args.map!{|a| Sequel.condition_specifier?(a) ? SQL::BooleanExpression.from_value_pairs(a) : a}
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

      # Returns true if the receiver is the same expression as the
      # the +other+ expression.
      def eql?(other)
        other.is_a?(self.class) && @op.eql?(other.op) && @args.eql?(other.args)
      end
      alias == eql?
      
      to_s_method :complex_expression_sql, '@op, @args'
    end

    # The base class for expressions that can be used in multiple places in
    # the SQL query.  
    class GenericExpression < Expression
    end
    
    ### Modules ###

    # Methods that create aliased identifiers
    module AliasMethods
      # Create an SQL column alias of the receiving column or expression to the given alias.
      def as(aliaz)
        AliasedExpression.new(self, aliaz)
      end
    end

    # This defines the bitwise methods: &, |, ^, ~, <<, and >>.  Because these
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
      # Cast the reciever to the given SQL type.  You can specify a ruby class as a type,
      # and it is handled similarly to using a database independent type in the schema methods.
      def cast(sql_type)
        Cast.new(self, sql_type)
      end

      # Cast the reciever to the given SQL type (or the database's default integer type if none given),
      # and return the result as a NumericExpression. 
      def cast_numeric(sql_type = nil)
        cast(sql_type || Integer).sql_number
      end

      # Cast the reciever to the given SQL type (or the database's default string type if none given),
      # and return the result as a StringExpression, so you can use +
      # directly on the result for SQL string concatenation.
      def cast_string(sql_type = nil)
        cast(sql_type || String).sql_string
      end
    end
    
    # Adds methods that allow you to treat an object as an instance of a specific
    # ComplexExpression subclass.  This is useful if another library
    # overrides the methods defined by Sequel.
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
      # Extract a datetime_part (e.g. year, month) from self:
      #
      #   :date.extract(:year) # SQL:  extract(year FROM "date")
      #
      # Also has the benefit of returning the result as a
      # NumericExpression instead of a generic ComplexExpression.
      #
      # The extract function is in the SQL standard, but it doesn't
      # doesn't use the standard function calling convention.
      def extract(datetime_part)
        Function.new(:extract, PlaceholderLiteralString.new("#{datetime_part} FROM ?", [self])).sql_number
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

    # Includes a method that returns Identifiers.
    module IdentifierMethods
      # Return self wrapped as an identifier.
      def identifier
        Identifier.new(self)
      end
    end

    # This module includes the methods that are defined on objects that can be 
    # used in a numeric or string context in SQL (Symbol (except on ruby 1.9), LiteralString, 
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
    # used in a string context in SQL (Symbol, LiteralString, SQL::Function,
    # and SQL::StringExpression).
    #
    # This defines the like (LIKE) and ilike methods, used for pattern matching.
    # like is case sensitive, ilike is case insensitive.
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

    # Methods that create Subscripts (SQL array accesses).
    module SubscriptMethods
      # Return an SQL array subscript with the given arguments.
      #
      #   :array.sql_subscript(1) # SQL: array[1]
      #   :array.sql_subscript(1, 2) # SQL: array[1, 2]
      def sql_subscript(*sub)
        Subscript.new(self, sub.flatten)
      end
    end

    class ComplexExpression
      include AliasMethods
      include CastMethods
      include OrderMethods
      include SubscriptMethods
    end

    class GenericExpression
      include AliasMethods
      include CastMethods
      include OrderMethods
      include ComplexExpressionMethods
      include BooleanMethods
      include NumericMethods
      include StringMethods
      include SubscriptMethods
      include InequalityMethods
    end

    ### Classes ###

    # Represents an aliasing of an expression/column to a given name.
    class AliasedExpression < Expression
      # The expression to alias
      attr_reader :expression

      # The alias to use for the expression, not alias since that is
      # a keyword in ruby.
      attr_reader :aliaz

      # Create an object with the given expression and alias.
      def initialize(expression, aliaz)
        @expression, @aliaz = expression, aliaz
      end

      to_s_method :aliased_expression_sql
    end

    # Blob is used to represent binary data in the Ruby environment that is
    # stored as a blob type in the database. Sequel represents binary data as a Blob object because 
    # certain database engines require binary data to be escaped.
    class Blob < ::String
      # Returns self
      def to_sequel_blob
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
          when NilClass, TrueClass, FalseClass
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
      # uninverted expression did not match, and vice-versa, except for possible issues with
      # SQL NULL (i.e. 1 == NULL is NULL and 1 != NULL is also NULL).
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
        raise(Sequel::Error, 'CaseExpression conditions must be a hash or array of all two pairs') unless Sequel.condition_specifier?(conditions)
        @conditions, @default, @expression = conditions.to_a, default, expression
      end

      to_s_method :case_expression_sql
    end

    # Represents a cast of an SQL expression to a specific type.
    class Cast < GenericExpression
      # The expression to cast
      attr_reader :expr

      # The type to which to cast the expression
      attr_reader :type
      
      # Set the attributes to the given arguments
      def initialize(expr, type)
        @expr = expr
        @type = type
      end

      to_s_method :cast_sql, '@expr, @type'
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
        x.class == self.class and @table == x.table
      end

      to_s_method :column_all_sql
    end
    
    # Represents constants or psuedo-constants (e.g. CURRENT_DATE) in SQL
    class Constant < GenericExpression
      # Create an object with the given table
      def initialize(constant)
        @constant = constant
      end
      
      to_s_method :constant_sql, '@constant'
    end
    
    module Constants
      CURRENT_DATE = Constant.new(:CURRENT_DATE)
      CURRENT_TIME = Constant.new(:CURRENT_TIME)
      CURRENT_TIMESTAMP = Constant.new(:CURRENT_TIMESTAMP)
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

      to_s_method :function_sql
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
      
      to_s_method :quote_identifier, '@value'
    end
    
    # Represents an SQL JOIN clause, used for joining tables.
    class JoinClause < Expression
      # The type of join to do
      attr_reader :join_type

      # The actual table to join
      attr_reader :table

      # The table alias to use for the join, if any
      attr_reader :table_alias

      # Create an object with the given join_type, table, and table alias
      def initialize(join_type, table, table_alias = nil)
        @join_type, @table, @table_alias = join_type, table, table_alias
      end

      to_s_method :join_clause_sql
    end

    # Represents an SQL JOIN table ON conditions clause.
    class JoinOnClause < JoinClause
      # The conditions for the join
      attr_reader :on

      # Create an object with the ON conditions and call super with the
      # remaining args.
      def initialize(on, *args)
        @on = on
        super(*args)
      end

      to_s_method :join_on_clause_sql
    end

    # Represents an SQL JOIN table USING (columns) clause.
    class JoinUsingClause < JoinClause
      # The columns that appear in both tables that should be equal 
      # for the conditions to match.
      attr_reader :using

      # Create an object with the given USING conditions and call super
      # with the remaining args.
      def initialize(using, *args)
        @using = using
        super(*args)
      end

      to_s_method :join_using_clause_sql
    end

    # Represents a literal string with placeholders and arguments.
    # This is necessary to ensure delayed literalization of the arguments
    # required for the prepared statement support
    class PlaceholderLiteralString < Expression
      # The arguments that will be subsituted into the placeholders.
      attr_reader :args

      # The literal string containing placeholders
      attr_reader :str

      # Whether to surround the expression with parantheses
      attr_reader :parens

      # Create an object with the given string, placeholder arguments, and parens flag.
      def initialize(str, args, parens=false)
        @str = str
        @args = args
        @parens = parens
      end

      to_s_method :placeholder_literal_string_sql
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
    class OrderedExpression < Expression
      # The expression to order the result set by.
      attr_reader :expression

      # Whether the expression should order the result set in a descending manner
      attr_reader :descending

      # Set the expression and descending attributes to the given values.
      def initialize(expression, descending = true)
        @expression, @descending = expression, descending
      end

      # Return a copy that is ASC
      def asc
        OrderedExpression.new(@expression, false)
      end

      # Return a copy that is DESC
      def desc
        OrderedExpression.new(@expression)
      end

      # Return an inverted expression, changing ASC to DESC and vice versa
      def invert
        OrderedExpression.new(@expression, !@descending)
      end

      to_s_method :ordered_expression_sql
    end

    # Represents a qualified (column with table or table with schema) reference. 
    class QualifiedIdentifier < GenericExpression
      include QualifyingMethods

      # The column to reference
      attr_reader :column

      # The table to reference
      attr_reader :table

      # Set the table and column to the given arguments
      def initialize(table, column)
        @table, @column = table, column
      end
      
      to_s_method :qualified_identifier_sql
    end
    
    # Subclass of ComplexExpression where the expression results
    # in a text/string/varchar value in SQL.
    class StringExpression < ComplexExpression
      include StringMethods
      include StringConcatenationMethods
      include InequalityMethods
      include NoBooleanInputMethods

      # Map of [regexp, case_insenstive] to ComplexExpression operator
      LIKE_MAP = {[true, true]=>:'~*', [true, false]=>:~, [false, true]=>:ILIKE, [false, false]=>:LIKE}
      
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
        l, lre, lci = like_element(l)
        lci = (ces.last.is_a?(Hash) ? ces.pop : {})[:case_insensitive] ? true : lci
        ces.collect! do |ce|
          r, rre, rci = like_element(ce)
          BooleanExpression.new(LIKE_MAP[[lre||rre, lci||rci]], l, r)
        end
        ces.length == 1 ? ces.at(0) : BooleanExpression.new(:OR, *ces)
      end
      
      # An array of three parts:
      # * The object to use
      # * Whether it is a regular expression
      # * Whether it is case insensitive
      def self.like_element(re) # :nodoc:
        if re.is_a?(Regexp)
          [re.source, true, re.casefold?]
        else
          [re, false, false]
        end
      end
      private_class_method :like_element
    end

    # Represents an SQL array.  Added so it is possible to deal with a
    # ruby array of all two pairs as an SQL array instead of an ordered
    # hash-like conditions specifier.
    class SQLArray < Expression
      # The array of objects this SQLArray wraps
      attr_reader :array

      # Create an object with the given array.
      def initialize(array)
        @array = array
      end

      to_s_method :array_sql, '@array'
    end

    # Represents an SQL array access, with multiple possible arguments.
    class Subscript < GenericExpression
      # The SQL array column
      attr_reader :f

      # The array of subscripts to use (should be an array of numbers)
      attr_reader :sub

      # Set the array column and subscripts to the given arguments
      def initialize(f, sub)
        @f, @sub = f, sub
      end

      # Create a new subscript appending the given subscript(s)
      # the the current array of subscripts.
      def |(sub)
        Subscript.new(@f, @sub + Array(sub))
      end
      
      to_s_method :subscript_sql
    end

    # The purpose of this class is to allow the easy creation of SQL identifiers and functions
    # without relying on methods defined on Symbol.  This is useful if another library defines
    # the methods defined by Sequel, or if you are running on ruby 1.9.
    #
    # An instance of this class is yielded to the block supplied to filter, order, and select.
    # If the block doesn't take an argument, the block is instance_evaled in the context of
    # a new instance of this class.
    #
    # VirtualRow uses method_missing to return Identifiers, QualifiedIdentifiers, Functions, or WindowFunctions, 
    # depending on how it is called.  If a block is not given, creates one of the following objects:
    # * Function - returned if any arguments are supplied, using the method name
    #   as the function name, and the arguments as the function arguments.
    # * QualifiedIdentifier - returned if the method name contains __, with the
    #   table being the part before __, and the column being the part after.
    # * Identifier - returned otherwise, using the method name.
    # If a block is given, it returns either a Function or WindowFunction, depending on the first
    # argument to the method.  Note that the block is currently not called by the code, though
    # this may change in a future version.  If the first argument is:
    # * no arguments given - uses a Function with no arguments.
    # * :* - uses a Function with a literal wildcard argument (*), mostly useful for COUNT.
    # * :distinct - uses a Function that prepends DISTINCT to the rest of the arguments, mostly
    #   useful for aggregate functions.
    # * :over - uses a WindowFunction.  If a second argument is provided, it should be a hash
    #   of options which are passed to Window (e.g. :window, :partition, :order, :frame).  The
    #   arguments to the function itself should be specified as :*=>true for a wildcard, or via
    #   the :args option.
    #
    # Examples:
    #
    #   ds = DB[:t]
    #   # Argument yielded to block
    #   ds.filter{|r| r.name < 2} # SELECT * FROM t WHERE (name < 2)
    #   # Block without argument (instance_eval)
    #   ds.filter{name < 2} # SELECT * FROM t WHERE (name < 2)
    #   # Qualified identifiers
    #   ds.filter{table__column + 1 < 2} # SELECT * FROM t WHERE ((table.column + 1) < 2)
    #   # Functions
    #   ds.filter{is_active(1, 'arg2')} # SELECT * FROM t WHERE is_active(1, 'arg2')
    #   ds.select{version{}} # SELECT version() FROM t
    #   ds.select{count(:*){}} # SELECT count(*) FROM t
    #   ds.select{count(:distinct, col1){}} # SELECT count(DISTINCT col1) FROM t
    #   # Window Functions
    #   ds.select{rank(:over){}} # SELECT rank() OVER () FROM t
    #   ds.select{count(:over, :*=>true){}} # SELECT count(*) OVER () FROM t
    #   ds.select{sum(:over, :args=>col1, :partition=>col2, :order=>col3){}} # SELECT sum(col1) OVER (PARTITION BY col2 ORDER BY col3) FROM t
    class VirtualRow < BasicObject
      WILDCARD = LiteralString.new('*').freeze
      QUESTION_MARK = LiteralString.new('?').freeze
      COMMA_SEPARATOR = LiteralString.new(', ').freeze
      DOUBLE_UNDERSCORE = '__'.freeze

      # Return Identifiers, QualifiedIdentifiers, Functions, or WindowFunctions, depending
      # on arguments and whether a block is provided.  Does not currently call the block.
      # See the class level documentation.
      def method_missing(m, *args, &block)
        if block
          if args.empty?
            Function.new(m)
          else
            case arg = args.shift
            when :*
              Function.new(m, WILDCARD)
            when :distinct
              Function.new(m, PlaceholderLiteralString.new("DISTINCT #{args.map{QUESTION_MARK}.join(COMMA_SEPARATOR)}", args))
            when :over
              opts = args.shift || {}
              fun_args = ::Kernel.Array(opts[:*] ? WILDCARD : opts[:args])
              WindowFunction.new(Function.new(m, *fun_args), Window.new(opts))
            else
              raise Error, 'unsupported VirtualRow method argument used with block'
            end
          end
        elsif args.empty?
          table, column = m.to_s.split(DOUBLE_UNDERSCORE, 2)
          column ? QualifiedIdentifier.new(table, column) : Identifier.new(m)
        else
          Function.new(m, *args)
        end
      end
    end

    # A window is part of a window function specifying the window over which the function operates.
    # It is separated from the WindowFunction class because it also can be used separately on
    # some databases.
    class Window < Expression
      # The options for this window.  Options currently used are:
      # * :frame - if specified, should be :all or :rows.  :all always operates over all rows in the
      #   partition, while :rows excludes the current row's later peers.  The default is to include
      #   all previous rows in the partition up to the current row's last peer.
      # * :order - order on the column(s) given
      # * :partition - partition/group on the column(s) given
      # * :window - base results on a previously specified named window
      attr_reader :opts

      # Set the options to the options given
      def initialize(opts={})
        @opts = opts
      end

      to_s_method :window_sql, '@opts'
    end

    # A WindowFunction is a grouping of a function with a window over which it operates.
    class WindowFunction < GenericExpression
      # The function to use, should be an SQL::Function.
      attr_reader :function

      # The window to use, should be an SQL::Window.
      attr_reader :window

      # Set the function and window.
      def initialize(function, window)
        @function, @window = function, window
      end

      to_s_method :window_function_sql, '@function, @window'
    end
  end

  # LiteralString is used to represent literal SQL expressions. A 
  # LiteralString is copied verbatim into an SQL statement. Instances of
  # LiteralString can be created by calling String#lit.
  class LiteralString
    include SQL::OrderMethods
    include SQL::ComplexExpressionMethods
    include SQL::BooleanMethods
    include SQL::NumericMethods
    include SQL::StringMethods
    include SQL::InequalityMethods
  end
  
  include SQL::Constants
end
