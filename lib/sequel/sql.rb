module Sequel
  if RUBY_VERSION < '1.9.0'
  # :nocov:
    # If on Ruby 1.8, create a <tt>Sequel::BasicObject</tt> class that is similar to the
    # the Ruby 1.9 +BasicObject+ class.  This is used in a few places where proxy
    # objects are needed that respond to any method call.
    class BasicObject
      # The instance methods to not remove from the class when removing
      # other methods.
      KEEP_METHODS = %w"__id__ __send__ __metaclass__ instance_eval instance_exec == equal? initialize method_missing"

      # Remove all but the most basic instance methods from the class.  A separate
      # method so that it can be called again if necessary if you load libraries
      # after Sequel that add instance methods to +Object+.
      def self.remove_methods!
        ((private_instance_methods + instance_methods) - KEEP_METHODS).each{|m| undef_method(m)}
      end
      remove_methods!
    end
  # :nocov:
  else
    # If on 1.9, create a <tt>Sequel::BasicObject</tt> class that is just like the
    # default +BasicObject+ class, except that missing constants are resolved in
    # +Object+.  This allows the virtual row support to work with classes
    # without prefixing them with ::, such as:
    #
    #   DB[:bonds].filter{maturity_date > Time.now}
    class BasicObject < ::BasicObject
      # Lookup missing constants in <tt>::Object</tt>
      def self.const_missing(name)
        ::Object.const_get(name)
      end

      # No-op method on ruby 1.9, which has a real +BasicObject+ class.
      def self.remove_methods!
      end
    end
  end

  class LiteralString < ::String
  end

  # Time subclass that gets literalized with only the time value, so it operates
  # like a standard SQL time type.
  class SQLTime < ::Time
    # Create a new SQLTime instance given an hour, minute, and second.
    def self.create(hour, minute, second, usec = 0)
      t = now
      local(t.year, t.month, t.day, hour, minute, second, usec)
    end

    # Return a string in HH:MM:SS format representing the time.
    def to_s(*args)
      if args.empty?
        strftime('%H:%M:%S')
      else
        # Superclass may have defined a method that takes a format string,
        # and we shouldn't override in that case.
        super
      end
    end
  end

  # The SQL module holds classes whose instances represent SQL fragments.
  # It also holds modules that are included in core ruby classes that
  # make Sequel a friendly DSL.
  module SQL

    ### Parent Classes ###

    # Classes/Modules aren't in alphabetical order due to the fact that
    # some reference constants defined in others at load time.

    # Base class for all SQL expression objects.
    class Expression
      @comparison_attrs = []

      class << self
        # All attributes used for equality and hash methods.
        attr_reader :comparison_attrs

        # Expression objects are assumed to be value objects, where their
        # attribute values can't change after assignment.  In order to make
        # it easy to define equality and hash methods, subclass
        # instances assume that the only values that affect the results of
        # such methods are the values of the object's attributes.
        def attr_reader(*args)
          super
          comparison_attrs.concat(args)
        end

        # Copy the comparison_attrs into the subclass.
        def inherited(subclass)
          super
          subclass.instance_variable_set(:@comparison_attrs, comparison_attrs.dup)
        end

          private

        # Create a to_s instance method that takes a dataset, and calls
        # the method provided on the dataset with args as the argument (self by default).
        # Used to DRY up some code.
        #
        # Do not call this method with untrusted input, as that can result in
        # arbitrary code execution.
        def to_s_method(meth, args=:self) # :nodoc:
          class_eval("def to_s_append(ds, sql) ds.#{meth}_append(sql, #{args}) end", __FILE__, __LINE__)
        end
      end

      # Alias of <tt>eql?</tt>
      def ==(other)
        eql?(other)
      end

      # Returns true if the receiver is the same expression as the
      # the +other+ expression.
      def eql?(other)
        other.is_a?(self.class) && !self.class.comparison_attrs.find{|a| send(a) != other.send(a)}
      end

      # Make sure that the hash value is the same if the attributes are the same.
      def hash
        ([self.class] + self.class.comparison_attrs.map{|x| send(x)}).hash
      end

      # Show the class name and instance variables for the object, necessary
      # for correct operation on ruby 1.9.2.
      def inspect
        "#<#{self.class} #{instance_variables.map{|iv| "#{iv}=>#{instance_variable_get(iv).inspect}"}.join(', ')}>"
      end

      # Returns +self+, because <tt>SQL::Expression</tt> already acts like +LiteralString+.
      def lit
        self
      end
      
      # Alias of +to_s+
      def sql_literal(ds)
        s = ''
        to_s_append(ds, s)
        s
      end
    end

    # Represents a complex SQL expression, with a given operator and one
    # or more attributes (which may also be ComplexExpressions, forming
    # a tree).  This class is the backbone of Sequel's ruby expression DSL.
    #
    # This is an abstract class that is not that useful by itself.  The
    # subclasses +BooleanExpression+, +NumericExpression+, and +StringExpression+
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

      # Standard mathematical operators used in +NumericMethods+
      MATHEMATICAL_OPERATORS = [:+, :-, :/, :*]

      # Bitwise mathematical operators used in +NumericMethods+
      BITWISE_OPERATORS = [:&, :|, :^, :<<, :>>, :%]

      # Operators that check for equality
      EQUALITY_OPERATORS = [:'=', :'!=']

      # Inequality operators used in +InequalityMethods+
      INEQUALITY_OPERATORS = [:<, :>, :<=, :>=]

      # Hash of ruby operator symbols to SQL operators, used in +BooleanMethods+
      BOOLEAN_OPERATOR_METHODS = {:& => :AND, :| =>:OR}

      # Operators that use IN/NOT IN for inclusion/exclusion
      IN_OPERATORS = [:IN, :'NOT IN']

      # Operators that use IS, used for special casing to override literal true/false values
      IS_OPERATORS = [:IS, :'IS NOT']

      # Operators that do pattern matching via regular expressions
      REGEXP_OPERATORS = [:~, :'!~', :'~*', :'!~*']
      
      # Operators that do pattern matching via LIKE
      LIKE_OPERATORS = [:LIKE, :'NOT LIKE', :ILIKE, :'NOT ILIKE']

      # Operator symbols that take exactly two arguments
      TWO_ARITY_OPERATORS = EQUALITY_OPERATORS + INEQUALITY_OPERATORS + IS_OPERATORS + IN_OPERATORS + REGEXP_OPERATORS + LIKE_OPERATORS

      # Operator symbols that take one or more arguments
      N_ARITY_OPERATORS = [:AND, :OR, :'||'] + MATHEMATICAL_OPERATORS + BITWISE_OPERATORS

      # Operator symbols that take only a single argument
      ONE_ARITY_OPERATORS = [:NOT, :NOOP, :'B~']

      # Custom expressions that may have different syntax on different databases
      CUSTOM_EXPRESSIONS = [:extract]

      # The operator symbol for this object
      attr_reader :op
      
      # An array of args for this object
      attr_reader :args

      # Set the operator symbol and arguments for this object to the ones given.
      # Convert all args that are hashes or arrays of two element arrays to +BooleanExpressions+,
      # other than the second arg for an IN/NOT IN operator.
      # Raise an +Error+ if the operator doesn't allow boolean input and a boolean argument is given.
      # Raise an +Error+ if the wrong number of arguments for a given operator is used.
      def initialize(op, *args)
        orig_args = args
        args = args.map{|a| Sequel.condition_specifier?(a) ? SQL::BooleanExpression.from_value_pairs(a) : a}
        case op
        when *N_ARITY_OPERATORS
          raise(Error, "The #{op} operator requires at least 1 argument") unless args.length >= 1
          old_args = args.map{|a| a.is_a?(self.class) && a.op == :NOOP ? a.args.first : a}
          args = []
          old_args.each{|a| a.is_a?(self.class) && a.op == op ? args.concat(a.args) : args.push(a)}
        when *TWO_ARITY_OPERATORS
          raise(Error, "The #{op} operator requires precisely 2 arguments") unless args.length == 2
          # With IN/NOT IN, even if the second argument is an array of two element arrays,
          # don't convert it into a boolean expression, since it's definitely being used
          # as a value list.
          args[1] = orig_args[1] if IN_OPERATORS.include?(op)
        when *ONE_ARITY_OPERATORS
          raise(Error, "The #{op} operator requires a single argument") unless args.length == 1
        when *CUSTOM_EXPRESSIONS
          # nothing
        else
          raise(Error, "Invalid operator #{op}")
        end
        @op = op
        @args = args
      end
      
      to_s_method :complex_expression_sql, '@op, @args'
    end

    # The base class for expressions that can be used in multiple places in
    # an SQL query.  
    class GenericExpression < Expression
    end
    
    ### Modules ###

    # Includes an +as+ method that creates an SQL alias.
    module AliasMethods
      # Create an SQL alias (+AliasedExpression+) of the receiving column or expression to the given alias.
      #
      #   Sequel.function(:func).as(:alias) # func() AS "alias"
      #   Sequel.function(:func).as(:alias, [:col_alias1, :col_alias2]) # func() AS "alias"("col_alias1", "col_alias2")
      def as(aliaz, columns=nil)
        AliasedExpression.new(self, aliaz, columns)
      end
    end

    # This defines the bitwise methods: &, |, ^, ~, <<, and >>.  Because these
    # methods overlap with the standard +BooleanMethods methods+, and they only
    # make sense for integers, they are only included in +NumericExpression+.
    #
    #   :a.sql_number & :b # "a" & "b"
    #   :a.sql_number | :b # "a" | "b"
    #   :a.sql_number ^ :b # "a" ^ "b"
    #   :a.sql_number << :b # "a" << "b"
    #   :a.sql_number >> :b # "a" >> "b"
    #   ~:a.sql_number # ~"a"
    module BitwiseMethods
      ComplexExpression::BITWISE_OPERATORS.each do |o|
        module_eval("def #{o}(o) NumericExpression.new(#{o.inspect}, self, o) end", __FILE__, __LINE__)
      end

      # Do the bitwise compliment of the self
      #
      #   ~:a.sql_number # ~"a"
      def ~
        NumericExpression.new(:'B~', self)
      end
    end

    # This module includes the boolean/logical AND (&), OR (|) and NOT (~) operators
    # that are defined on objects that can be used in a boolean context in SQL
    # (+Symbol+, +LiteralString+, and <tt>SQL::GenericExpression</tt>).
    #
    #   :a & :b # "a" AND "b"
    #   :a | :b # "a" OR "b"
    #   ~:a # NOT "a"
    #
    # One exception to this is when a NumericExpression or Integer is the argument
    # to & or |, in which case a bitwise method will be used:
    #
    #   :a & 1 # "a" & 1 
    #   :a | (:b + 1) # "a" | ("b" + 1)
    module BooleanMethods
      ComplexExpression::BOOLEAN_OPERATOR_METHODS.each do |m, o|
        module_eval(<<-END, __FILE__, __LINE__+1)
          def #{m}(o)
            case o
            when NumericExpression, Integer
              NumericExpression.new(#{m.inspect}, self, o)
            else  
              BooleanExpression.new(#{o.inspect}, self, o)
            end
          end
        END
      end
      
      # Create a new BooleanExpression with NOT, representing the inversion of whatever self represents.
      #
      #   ~:a # NOT :a
      def ~
        BooleanExpression.invert(self)
      end
    end

    # These methods are designed as replacements for the core extensions, so that
    # Sequel is still easy to use if the core extensions are not enabled.
    module Builders
      # Create an SQL::AliasedExpression for the given expression and alias.
      #
      #   Sequel.as(:column, :alias) # "column" AS "alias"
      #   Sequel.as(:column, :alias, [:col_alias1, :col_alias2]) # "column" AS "alias"("col_alias1", "col_alias2")
      def as(exp, aliaz, columns=nil)
        SQL::AliasedExpression.new(exp, aliaz, columns)
      end

      # Order the given argument ascending.
      # Options:
      #
      # :nulls :: Set to :first to use NULLS FIRST (so NULL values are ordered
      #           before other values), or :last to use NULLS LAST (so NULL values
      #           are ordered after other values).
      #
      #   Sequel.asc(:a) # a ASC
      #   Sequel.asc(:b, :nulls=>:last) # b ASC NULLS LAST
      def asc(arg, opts=OPTS)
        SQL::OrderedExpression.new(arg, false, opts)
      end

      # Return an <tt>SQL::Blob</tt> that holds the same data as this string.
      # Blobs provide proper escaping of binary data.  If given a blob, returns it
      # directly.
      def blob(s)
        if s.is_a?(SQL::Blob)
          s
        else
          SQL::Blob.new(s)
        end
      end

      # Return an <tt>SQL::CaseExpression</tt> created with the given arguments.
      #
      #   Sequel.case([[{:a=>[2,3]}, 1]], 0) # SQL: CASE WHEN a IN (2, 3) THEN 1 ELSE 0 END
      #   Sequel.case({:a=>1}, 0, :b) # SQL: CASE b WHEN a THEN 1 ELSE 0 END
      def case(*args) # core_sql ignore
        SQL::CaseExpression.new(*args)
      end

      # Cast the reciever to the given SQL type.  You can specify a ruby class as a type,
      # and it is handled similarly to using a database independent type in the schema methods.
      #
      #   Sequel.cast(:a, :integer) # CAST(a AS integer)
      #   Sequel.cast(:a, String) # CAST(a AS varchar(255))
      def cast(arg, sql_type)
        SQL::Cast.new(arg, sql_type)
      end

      # Cast the reciever to the given SQL type (or the database's default Integer type if none given),
      # and return the result as a +NumericExpression+, so you can use the bitwise operators
      # on the result. 
      #
      #   Sequel.cast_numeric(:a) # CAST(a AS integer)
      #   Sequel.cast_numeric(:a, Float) # CAST(a AS double precision)
      def cast_numeric(arg, sql_type = nil)
        cast(arg, sql_type || Integer).sql_number
      end

      # Cast the reciever to the given SQL type (or the database's default String type if none given),
      # and return the result as a +StringExpression+, so you can use +
      # directly on the result for SQL string concatenation.
      #
      #   Sequel.cast_string(:a) # CAST(a AS varchar(255))
      #   Sequel.cast_string(:a, :text) # CAST(a AS text)
      def cast_string(arg, sql_type = nil)
        cast(arg, sql_type || String).sql_string
      end

      # Return an emulated function call for getting the number of characters
      # in the argument:
      #
      #   Sequel.char_length(:a) # char_length(a) -- Most databases
      #   Sequel.char_length(:a) # length(a) -- SQLite
      def char_length(arg)
        SQL::Function.new!(:char_length, [arg], :emulate=>true)
      end

      # Do a deep qualification of the argument using the qualifier.  This recurses into
      # nested structures.
      #
      #   Sequel.deep_qualify(:table, :column) # "table"."column"
      #   Sequel.deep_qualify(:table, Sequel.+(:column, 1)) # "table"."column" + 1
      #   Sequel.deep_qualify(:table, Sequel.like(:a, 'b')) # "table"."a" LIKE 'b' ESCAPE '\'
      def deep_qualify(qualifier, expr)
        Sequel::Qualifier.new(Sequel, qualifier).transform(expr)
      end

      # Return a delayed evaluation that uses the passed block. This is used
      # to delay evaluations of the code to runtime.  For example, with
      # the following code:
      #
      #   ds = DB[:table].where{column > Time.now}
      #
      # The filter is fixed to the time that where was called. Unless you are
      # only using the dataset once immediately after creating it, that's
      # probably not desired.  If you just want to set it to the time when the
      # query is sent to the database, you can wrap it in Sequel.delay:
      #
      #   ds = DB[:table].where{column > Sequel.delay{Time.now}}
      #
      # Note that for dates and timestamps, you are probably better off using
      # Sequel::CURRENT_DATE and Sequel::CURRENT_TIMESTAMP instead of this
      # generic delayed evaluation facility.
      def delay(&block)
        raise(Error, "Sequel.delay requires a block") unless block
        SQL::DelayedEvaluation.new(block)
      end

      # Order the given argument descending.
      # Options:
      #
      # :nulls :: Set to :first to use NULLS FIRST (so NULL values are ordered
      #           before other values), or :last to use NULLS LAST (so NULL values
      #           are ordered after other values).
      #
      #   Sequel.desc(:a) # b DESC
      #   Sequel.desc(:b, :nulls=>:first) # b DESC NULLS FIRST
      def desc(arg, opts=OPTS)
        SQL::OrderedExpression.new(arg, true, opts)
      end

      # Wraps the given object in an appropriate Sequel wrapper.
      # If the given object is already a Sequel object, return it directly.
      # For condition specifiers (hashes and arrays of two pairs), true, and false,
      # return a boolean expressions.  For numeric objects, return a numeric
      # expression.  For strings, return a string expression.  For procs or when
      # the method is passed a block, evaluate it as a virtual row and wrap it
      # appropriately.  In all other cases, use a generic wrapper.
      #
      # This method allows you to construct SQL expressions that are difficult
      # to construct via other methods.  For example:
      #
      #   Sequel.expr(1) - :a # SQL: (1 - a)
      def expr(arg=(no_arg=true), &block)
        if block_given?
          if no_arg
            return expr(block)
          else
            raise Error, 'cannot provide both an argument and a block to Sequel.expr'
          end
        elsif no_arg
          raise Error, 'must provide either an argument or a block to Sequel.expr'
        end

        case arg
        when Symbol
          t, c, a = Sequel.split_symbol(arg)

          arg = if t
            SQL::QualifiedIdentifier.new(t, c)
          else
            SQL::Identifier.new(c)
          end

          if a
            arg = SQL::AliasedExpression.new(arg, a)
          end

          arg
        when SQL::Expression, LiteralString, SQL::Blob
          arg
        when Hash
          SQL::BooleanExpression.from_value_pairs(arg, :AND)
        when Array
          if condition_specifier?(arg)
            SQL::BooleanExpression.from_value_pairs(arg, :AND)
          else
            SQL::Wrapper.new(arg)
          end
        when Numeric
          SQL::NumericExpression.new(:NOOP, arg)
        when String
          SQL::StringExpression.new(:NOOP, arg)
        when TrueClass, FalseClass
          SQL::BooleanExpression.new(:NOOP, arg)
        when Proc
          expr(virtual_row(&arg))
        else
          SQL::Wrapper.new(arg)
        end
      end

      # Extract a datetime_part (e.g. year, month) from the given
      # expression:
      #
      #   Sequel.extract(:year, :date) # extract(year FROM "date")
      def extract(datetime_part, exp)
        SQL::NumericExpression.new(:extract, datetime_part, exp)
      end

      # Returns a <tt>Sequel::SQL::Function</tt> with the function name
      # and the given arguments.
      #
      #   Sequel.function(:now) # SQL: now()
      #   Sequel.function(:substr, :a, 1) # SQL: substr(a, 1)
      def function(name, *args)
        SQL::Function.new(name, *args)
      end

      # Return the argument wrapped as an <tt>SQL::Identifier</tt>.
      #
      #   Sequel.identifier(:a__b) # "a__b"
      def identifier(name)
        SQL::Identifier.new(name)
      end

      # Return a <tt>Sequel::SQL::StringExpression</tt> representing an SQL string made up of the
      # concatenation of the given array's elements.  If an argument is passed,
      # it is used in between each element of the array in the SQL
      # concatenation.
      #
      #   Sequel.join([:a]) # SQL: a
      #   Sequel.join([:a, :b]) # SQL: a || b
      #   Sequel.join([:a, 'b']) # SQL: a || 'b'
      #   Sequel.join(['a', :b], ' ') # SQL: 'a' || ' ' || b
      def join(args, joiner=nil)
        raise Error, 'argument to Sequel.join must be an array' unless args.is_a?(Array)
        if joiner
          args = args.zip([joiner]*args.length).flatten
          args.pop
        end

        return SQL::StringExpression.new(:NOOP, '') if args.empty?

        args = args.map do |a|
          case a
          when Symbol, ::Sequel::SQL::Expression, ::Sequel::LiteralString, TrueClass, FalseClass, NilClass
            a
          else
            a.to_s
          end
        end
        SQL::StringExpression.new(:'||', *args)
      end

      # Create a <tt>BooleanExpression</tt> case insensitive (if the database supports it) pattern match of the receiver with
      # the given patterns.  See <tt>SQL::StringExpression.like</tt>.
      #
      #   Sequel.ilike(:a, 'A%') # "a" ILIKE 'A%' ESCAPE '\'
      def ilike(*args)
        SQL::StringExpression.like(*(args << {:case_insensitive=>true}))
      end

      # Create a <tt>SQL::BooleanExpression</tt> case sensitive (if the database supports it) pattern match of the receiver with
      # the given patterns.  See <tt>SQL::StringExpression.like</tt>.
      #
      #   Sequel.like(:a, 'A%') # "a" LIKE 'A%' ESCAPE '\'
      def like(*args)
        SQL::StringExpression.like(*args)
      end

      # Converts a string into a <tt>Sequel::LiteralString</tt>, in order to override string
      # literalization, e.g.:
      #
      #   DB[:items].filter(:abc => 'def').sql #=>
      #     "SELECT * FROM items WHERE (abc = 'def')"
      #
      #   DB[:items].filter(:abc => Sequel.lit('def')).sql #=>
      #     "SELECT * FROM items WHERE (abc = def)"
      #
      # You can also provide arguments, to create a <tt>Sequel::SQL::PlaceholderLiteralString</tt>:
      #
      #    DB[:items].select{|o| o.count(Sequel.lit('DISTINCT ?', :a))}.sql #=>
      #      "SELECT count(DISTINCT a) FROM items"
      def lit(s, *args) # core_sql ignore
        if args.empty?
          if s.is_a?(LiteralString)
            s
          else
            LiteralString.new(s)
          end
        else
          SQL::PlaceholderLiteralString.new(s, args) 
        end
      end

      # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from the condition
      # specifier, matching none of the conditions.
      #
      #   Sequel.negate(:a=>true) # SQL: a IS NOT TRUE
      #   Sequel.negate([[:a, true]]) # SQL: a IS NOT TRUE
      #   Sequel.negate([[:a, 1], [:b, 2]]) # SQL: ((a != 1) AND (b != 2))
      def negate(arg)
        if condition_specifier?(arg)
          SQL::BooleanExpression.from_value_pairs(arg, :AND, true)
        else
          raise Error, 'must pass a conditions specifier to Sequel.negate'
        end
      end

      # Return a <tt>Sequel::SQL::BooleanExpression</tt> created from the condition
      # specifier, matching any of the conditions.
      #
      #   Sequel.or(:a=>true) # SQL: a IS TRUE
      #   Sequel.or([[:a, true]]) # SQL: a IS TRUE
      #   Sequel.or([[:a, 1], [:b, 2]]) # SQL: ((a = 1) OR (b = 2))
      def or(arg)
        if condition_specifier?(arg)
          SQL::BooleanExpression.from_value_pairs(arg, :OR, false)
        else
          raise Error, 'must pass a conditions specifier to Sequel.or'
        end
      end

      # Create a qualified identifier with the given qualifier and identifier
      #
      #   Sequel.qualify(:table, :column) # "table"."column"
      #   Sequel.qualify(:schema, :table) # "schema"."table"
      #   Sequel.qualify(:table, :column).qualify(:schema) # "schema"."table"."column"
      def qualify(qualifier, identifier)
        SQL::QualifiedIdentifier.new(qualifier, identifier)
      end

      # Return an <tt>SQL::Subscript</tt> with the given arguments, representing an
      # SQL array access.
      #
      #   Sequel.subscript(:array, 1) # array[1]
      #   Sequel.subscript(:array, 1, 2) # array[1, 2]
      #   Sequel.subscript(:array, [1, 2]) # array[1, 2]
      #   Sequel.subscript(:array, 1..2) # array[1:2]
      #   Sequel.subscript(:array, 1...3) # array[1:2]
      def subscript(exp, *subs)
        SQL::Subscript.new(exp, subs.flatten)
      end

      # Return an emulated function call for trimming a string of spaces from
      # both sides (similar to ruby's String#strip).
      #
      #   Sequel.trim(:a) # trim(a) -- Most databases
      #   Sequel.trim(:a) # ltrim(rtrim(a)) -- Microsoft SQL Server
      def trim(arg)
        SQL::Function.new!(:trim, [arg], :emulate=>true)
      end

      # Return a <tt>SQL::ValueList</tt> created from the given array.  Used if the array contains
      # all two element arrays and you want it treated as an SQL value list (IN predicate) 
      # instead of as a conditions specifier (similar to a hash).  This is not necessary if you are using
      # this array as a value in a filter, but may be necessary if you are using it as a
      # value with placeholder SQL:
      #
      #   DB[:a].filter([:a, :b]=>[[1, 2], [3, 4]]) # SQL: (a, b) IN ((1, 2), (3, 4))
      #   DB[:a].filter('(a, b) IN ?', [[1, 2], [3, 4]]) # SQL: (a, b) IN ((1 = 2) AND (3 = 4))
      #   DB[:a].filter('(a, b) IN ?', Sequel.value_list([[1, 2], [3, 4]])) # SQL: (a, b) IN ((1, 2), (3, 4))
      def value_list(arg)
        raise Error, 'argument to Sequel.value_list must be an array' unless arg.is_a?(Array)
        SQL::ValueList.new(arg)
      end
    end

    # Holds methods that are used to cast objects to different SQL types.
    module CastMethods 
      # Cast the reciever to the given SQL type.  You can specify a ruby class as a type,
      # and it is handled similarly to using a database independent type in the schema methods.
      #
      #   Sequel.function(:func).cast(:integer) # CAST(func() AS integer)
      #   Sequel.function(:func).cast(String) # CAST(func() AS varchar(255))
      def cast(sql_type)
        Cast.new(self, sql_type)
      end

      # Cast the reciever to the given SQL type (or the database's default Integer type if none given),
      # and return the result as a +NumericExpression+, so you can use the bitwise operators
      # on the result. 
      #
      #   Sequel.function(:func).cast_numeric # CAST(func() AS integer)
      #   Sequel.function(:func).cast_numeric(Float) # CAST(func() AS double precision)
      def cast_numeric(sql_type = nil)
        Cast.new(self, sql_type || Integer).sql_number
      end

      # Cast the reciever to the given SQL type (or the database's default String type if none given),
      # and return the result as a +StringExpression+, so you can use +
      # directly on the result for SQL string concatenation.
      #
      #   Sequel.function(:func).cast_string # CAST(func() AS varchar(255))
      #   Sequel.function(:func).cast_string(:text) # CAST(func() AS text)
      def cast_string(sql_type = nil)
        Cast.new(self, sql_type || String).sql_string
      end
    end

    # Adds methods that allow you to treat an object as an instance of a specific
    # +ComplexExpression+ subclass.  This is useful if another library
    # overrides the methods defined by Sequel.
    #
    # For example, if <tt>Symbol#/</tt> is overridden to produce a string (for
    # example, to make file system path creation easier), the
    # following code will not do what you want:
    #
    #   :price/10 > 100
    #
    # In that case, you need to do the following:
    #
    #   :price.sql_number/10 > 100
    module ComplexExpressionMethods
      # Extract a datetime part (e.g. year, month) from self:
      #
      #   :date.extract(:year) # extract(year FROM "date")
      #
      # Also has the benefit of returning the result as a
      # NumericExpression instead of a generic ComplexExpression.
      def extract(datetime_part)
        NumericExpression.new(:extract, datetime_part, self)
      end

      # Return a BooleanExpression representation of +self+.
      def sql_boolean
        BooleanExpression.new(:NOOP, self)
      end

      # Return a NumericExpression representation of +self+.
      # 
      #   ~:a # NOT "a"
      #   ~:a.sql_number # ~"a"
      def sql_number
        NumericExpression.new(:NOOP, self)
      end

      # Return a StringExpression representation of +self+.
      #
      #   :a + :b # "a" + "b"
      #   :a.sql_string + :b # "a" || "b"
      def sql_string
        StringExpression.new(:NOOP, self)
      end
    end

    # This module includes the inequality methods (>, <, >=, <=) that are defined on objects that can be 
    # used in a numeric or string context in SQL.
    #
    #   Sequel.lit('a') > :b # a > "b"
    #   Sequel.lit('a') < :b # a > "b"
    #   Sequel.lit('a') >= :b # a >= "b"
    #   Sequel.lit('a') <= :b # a <= "b"
    module InequalityMethods
      ComplexExpression::INEQUALITY_OPERATORS.each do |o|
        module_eval("def #{o}(o) BooleanExpression.new(#{o.inspect}, self, o) end", __FILE__, __LINE__)
      end
    end

    # This module includes the standard mathematical methods (+, -, *, and /)
    # that are defined on objects that can be used in a numeric context in SQL
    # (+Symbol+, +LiteralString+, and +SQL::GenericExpression+).
    #
    #   :a + :b # "a" + "b"
    #   :a - :b # "a" - "b"
    #   :a * :b # "a" * "b"
    #   :a / :b # "a" / "b"
    #
    # One exception to this is if + is called with a +String+ or +StringExpression+,
    # in which case the || operator is used instead of the + operator:
    #
    #   :a + 'b' # "a" || 'b'
    module NumericMethods
      ComplexExpression::MATHEMATICAL_OPERATORS.each do |o|
        module_eval("def #{o}(o) NumericExpression.new(#{o.inspect}, self, o) end", __FILE__, __LINE__) unless o == :+
      end

      # Use || as the operator when called with StringExpression and String instances,
      # and the + operator for LiteralStrings and all other types.
      def +(ce)
        case ce
        when LiteralString
          NumericExpression.new(:+, self, ce)
        when StringExpression, String
          StringExpression.new(:'||', self, ce)
        else
          NumericExpression.new(:+, self, ce)
        end
      end
    end

    # These methods are designed as replacements for the core extension operator
    # methods, so that Sequel is still easy to use if the core extensions are not
    # enabled.
    #
    # The following methods are defined via metaprogramming: +, -, *, /, &, |.
    # The +, -, *, and / operators return numeric expressions combining all the
    # arguments with the appropriate operator, and the & and | operators return
    # boolean expressions combining all of the arguments with either AND or OR.
    module OperatorBuilders
      {'::Sequel::SQL::NumericExpression'=>{'+'=>'+', '-'=>'-', '*'=>'*', '/'=>'/'},
       '::Sequel::SQL::BooleanExpression'=>{'&'=>'AND', '|'=>'OR'}}.each do |klass, ops|
        ops.each do |m, op|
          class_eval(<<-END, __FILE__, __LINE__ + 1)
            def #{m}(*args)
              if (args.length == 1)
                if (v = args.first).class.is_a?(#{klass})
                  v
                else
                  #{klass}.new(:NOOP, v)
                end
              else
                #{klass}.new(:#{op}, *args)
              end
            end
          END
        end
      end
      
      # Invert the given expression.  Returns a <tt>Sequel::SQL::BooleanExpression</tt>
      # created from this argument, not matching all of the conditions.
      #
      #   Sequel.~(nil) # SQL: NOT NULL
      #   Sequel.~([[:a, true]]) # SQL: a IS NOT TRUE
      #   Sequel.~([[:a, 1], [:b, [2, 3]]]) # SQL: a != 1 OR b NOT IN (2, 3)
      def ~(arg)
        if condition_specifier?(arg)
          SQL::BooleanExpression.from_value_pairs(arg, :OR, true)
        else
          SQL::BooleanExpression.invert(arg)
        end
      end
    end

    # Methods that create +OrderedExpressions+, used for sorting by columns
    # or more complex expressions.
    module OrderMethods
      # Mark the receiving SQL column as sorting in an ascending fashion (generally a no-op).
      # Options:
      #
      # :nulls :: Set to :first to use NULLS FIRST (so NULL values are ordered
      #           before other values), or :last to use NULLS LAST (so NULL values
      #           are ordered after other values).
      def asc(opts=OPTS)
        OrderedExpression.new(self, false, opts)
      end
      
      # Mark the receiving SQL column as sorting in a descending fashion.
      # Options:
      #
      # :nulls :: Set to :first to use NULLS FIRST (so NULL values are ordered
      #           before other values), or :last to use NULLS LAST (so NULL values
      #           are ordered after other values).
      def desc(opts=OPTS)
        OrderedExpression.new(self, true, opts)
      end
    end

    # Includes a +qualify+ method that created <tt>QualifiedIdentifier</tt>s, used for qualifying column
    # names with a table or table names with a schema, and the * method for returning all columns in
    # the identifier if no arguments are given.
    module QualifyingMethods
      # If no arguments are given, return an SQL::ColumnAll:
      #
      #   Sequel.expr(:a__b).*  # a.b.*
      def *(ce=(arg=false;nil))
        if arg == false
          Sequel::SQL::ColumnAll.new(self)
        else
          super(ce)
        end
      end

      # Qualify the receiver with the given +qualifier+ (table for column/schema for table).
      #
      #   Sequel.expr(:column).qualify(:table) # "table"."column"
      #   Sequel.expr(:table).qualify(:schema) # "schema"."table"
      #   Sequel.qualify(:table, :column).qualify(:schema) # "schema"."table"."column"
      def qualify(qualifier)
        QualifiedIdentifier.new(qualifier, self)
      end
    end

    # This module includes the +like+ and +ilike+ methods used for pattern matching that are defined on objects that can be 
    # used in a string context in SQL (+Symbol+, +LiteralString+, <tt>SQL::GenericExpression</tt>).
    module StringMethods
      # Create a +BooleanExpression+ case insensitive pattern match of the receiver
      # with the given patterns.  See <tt>StringExpression.like</tt>.
      #
      #   :a.ilike('A%') # "a" ILIKE 'A%' ESCAPE '\'
      def ilike(*ces)
        StringExpression.like(self, *(ces << {:case_insensitive=>true}))
      end

      # Create a +BooleanExpression+ case sensitive (if the database supports it) pattern match of the receiver with
      # the given patterns.  See <tt>StringExpression.like</tt>.
      #
      #   :a.like('A%') # "a" LIKE 'A%' ESCAPE '\'
      def like(*ces)
        StringExpression.like(self, *ces)
      end
    end

    # This module includes the <tt>+</tt> method.  It is included in +StringExpression+ and can be included elsewhere
    # to allow the use of the + operator to represent concatenation of SQL Strings:
    module StringConcatenationMethods
      # Return a +StringExpression+ representing the concatenation of the receiver
      # with the given argument.
      #
      #   :x.sql_string + :y # => "x" || "y"
      def +(ce)
        StringExpression.new(:'||', self, ce)
      end
    end

    # This module includes the +sql_subscript+ method, representing SQL array accesses.
    module SubscriptMethods
      # Return a <tt>Subscript</tt> with the given arguments, representing an
      # SQL array access.
      #
      #   :array.sql_subscript(1) # array[1]
      #   :array.sql_subscript(1, 2) # array[1, 2]
      #   :array.sql_subscript([1, 2]) # array[1, 2]
      #   :array.sql_subscript(:array, 1..2) # array[1:2]
      #   :array.sql_subscript(:array, 1...3) # array[1:2]
      def sql_subscript(*sub)
        Subscript.new(self, sub.flatten)
      end
    end

    ### Classes ###

    # Represents an aliasing of an expression to a given alias.
    class AliasedExpression < Expression
      # The expression to alias
      attr_reader :expression

      # The alias to use for the expression, not +alias+ since that is
      # a keyword in ruby.
      attr_reader :aliaz
      alias_method :alias, :aliaz

      # The columns aliases to use, for when the aliased expression is
      # a record or set of records (such as a dataset). 
      attr_reader :columns

      # Create an object with the given expression and alias.
      def initialize(expression, aliaz, columns=nil)
        @expression = expression
        @aliaz = aliaz
        @columns = columns
      end

      to_s_method :aliased_expression_sql
    end

    # +Blob+ is used to represent binary data in the Ruby environment that is
    # stored as a blob type in the database. Sequel represents binary data as a Blob object because 
    # most database engines require binary data to be escaped differently than regular strings.
    class Blob < ::String
      include SQL::AliasMethods
      include SQL::CastMethods

      # Return a LiteralString with the same content if no args are given, otherwise
      # return a SQL::PlaceholderLiteralString with the current string and the given args.
      def lit(*args)
        args.empty? ? LiteralString.new(self) : SQL::PlaceholderLiteralString.new(self, args)
      end
      
      # Returns +self+, since it is already a blob.
      def to_sequel_blob
        self
      end
    end

    # Subclass of +ComplexExpression+ where the expression results
    # in a boolean value in SQL.
    class BooleanExpression < ComplexExpression
      include BooleanMethods
      
      # Take pairs of values (e.g. a hash or array of two element arrays)
      # and converts it to a +BooleanExpression+.  The operator and args
      # used depends on the case of the right (2nd) argument:
      #
      # 0..10 :: left >= 0 AND left <= 10
      # [1,2] :: left IN (1,2)
      # nil :: left IS NULL
      # true :: left IS TRUE 
      # false :: left IS FALSE 
      # /as/ :: left ~ 'as'
      # :blah :: left = blah
      # 'blah' :: left = 'blah'
      #
      # If multiple arguments are given, they are joined with the op given (AND
      # by default, OR possible).  If negate is set to true,
      # all subexpressions are inverted before used.  Therefore, the following
      # expressions are equivalent:
      #
      #   ~from_value_pairs(hash)
      #   from_value_pairs(hash, :OR, true)
      def self.from_value_pairs(pairs, op=:AND, negate=false)
        pairs = pairs.map{|l,r| from_value_pair(l, r)}
        pairs.map!{|ce| invert(ce)} if negate
        pairs.length == 1 ? pairs.at(0) : new(op, *pairs)
      end

      # Return a BooleanExpression based on the right side of the pair.
      def self.from_value_pair(l, r)
        case r
        when Range
          new(:AND, new(:>=, l, r.begin), new(r.exclude_end? ? :< : :<=, l, r.end))
        when ::Array, ::Sequel::Dataset
          new(:IN, l, r)
        when NegativeBooleanConstant
          new(:"IS NOT", l, r.constant)
        when BooleanConstant
          new(:IS, l, r.constant)
        when NilClass, TrueClass, FalseClass
          new(:IS, l, r)
        when Regexp
          StringExpression.like(l, r)
        when DelayedEvaluation
          Sequel.delay{|ds| from_value_pair(l, r.call(ds))}
        when Dataset::PlaceholderLiteralizer::Argument
          r.transform{|v| from_value_pair(l, v)}
        else
          new(:'=', l, r)
        end
      end
      private_class_method :from_value_pair
      
      # Invert the expression, if possible.  If the expression cannot
      # be inverted, raise an error.  An inverted expression should match everything that the
      # uninverted expression did not match, and vice-versa, except for possible issues with
      # SQL NULL (i.e. 1 == NULL is NULL and 1 != NULL is also NULL).
      #
      #   BooleanExpression.invert(:a) # NOT "a"
      def self.invert(ce)
        case ce
        when BooleanExpression
          case op = ce.op
          when :AND, :OR
            BooleanExpression.new(OPERTATOR_INVERSIONS[op], *ce.args.collect{|a| BooleanExpression.invert(a)})
          else
            BooleanExpression.new(OPERTATOR_INVERSIONS[op], *ce.args.dup)
          end
        when StringExpression, NumericExpression
          raise(Sequel::Error, "cannot invert #{ce.inspect}")
        when Constant
          CONSTANT_INVERSIONS[ce] || raise(Sequel::Error, "cannot invert #{ce.inspect}")
        else
          BooleanExpression.new(:NOT, ce)
        end
      end

      # Always use an AND operator for & on BooleanExpressions
      def &(ce)
        BooleanExpression.new(:AND, self, ce)
      end

      # Always use an OR operator for | on BooleanExpressions
      def |(ce)
        BooleanExpression.new(:OR, self, ce)
      end

      # Return self instead of creating a new object to save on memory.
      def sql_boolean
        self
      end
    end

    # Represents an SQL CASE expression, used for conditional branching in SQL.
    class CaseExpression < GenericExpression
      # An array of all two pairs with the first element specifying the
      # condition and the second element specifying the result if the
      # condition matches.
      attr_reader :conditions

      # The default value if no conditions match. 
      attr_reader :default

      # The expression to test the conditions against
      attr_reader :expression

      # Create an object with the given conditions and
      # default value.  An expression can be provided to
      # test each condition against, instead of having
      # all conditions represent their own boolean expression.
      def initialize(conditions, default, expression=(no_expression=true; nil))
        raise(Sequel::Error, 'CaseExpression conditions must be a hash or array of all two pairs') unless Sequel.condition_specifier?(conditions)
        @conditions, @default, @expression, @no_expression = conditions.to_a, default, expression, no_expression
      end

      # Whether to use an expression for this CASE expression.
      def expression?
        !@no_expression
      end

      # Merge the CASE expression into the conditions, useful for databases that
      # don't support CASE expressions.
      def with_merged_expression
        if expression?
          e = expression
          CaseExpression.new(conditions.map{|c, r| [::Sequel::SQL::BooleanExpression.new(:'=', e, c), r]}, default)
        else
          self
        end
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

      to_s_method :column_all_sql
    end
    
    class ComplexExpression
      include AliasMethods
      include CastMethods
      include OrderMethods
      include SubscriptMethods

      # Return a BooleanExpression with the same op and args.
      def sql_boolean
        BooleanExpression.new(self.op, *self.args)
      end

      # Return a NumericExpression with the same op and args.
      def sql_number
        NumericExpression.new(self.op, *self.args)
      end

      # Return a StringExpression with the same op and args.
      def sql_string
        StringExpression.new(self.op, *self.args)
      end
    end

    # Represents constants or psuedo-constants (e.g. +CURRENT_DATE+) in SQL.
    class Constant < GenericExpression
      # The underlying constant related to this object.
      attr_reader :constant

      # Create an constant with the given value
      def initialize(constant)
        @constant = constant
      end
      
      to_s_method :constant_sql, '@constant'
    end

    # Represents boolean constants such as +NULL+, +NOTNULL+, +TRUE+, and +FALSE+.
    class BooleanConstant < Constant
      to_s_method :boolean_constant_sql, '@constant'
    end
    
    # Represents inverse boolean constants (currently only +NOTNULL+). A
    # special class to allow for special behavior.
    class NegativeBooleanConstant < Constant
      to_s_method :negative_boolean_constant_sql, '@constant'
    end
    
    # Holds default generic constants that can be referenced.  These
    # are included in the Sequel top level module and are also available
    # in this module which can be required at the top level to get
    # direct access to the constants.
    module Constants
      CURRENT_DATE = Constant.new(:CURRENT_DATE)
      CURRENT_TIME = Constant.new(:CURRENT_TIME)
      CURRENT_TIMESTAMP = Constant.new(:CURRENT_TIMESTAMP)
      SQLTRUE = TRUE = BooleanConstant.new(true)
      SQLFALSE = FALSE = BooleanConstant.new(false)
      NULL = BooleanConstant.new(nil)
      NOTNULL = NegativeBooleanConstant.new(nil)
    end

    class ComplexExpression
      # A hash of the opposite for each constant, used for inverting constants.
      CONSTANT_INVERSIONS = {Constants::TRUE=>Constants::FALSE, Constants::FALSE=>Constants::TRUE,
                             Constants::NULL=>Constants::NOTNULL, Constants::NOTNULL=>Constants::NULL}
    end

    # Represents a delayed evaluation, encapsulating a callable
    # object which returns the value to use when called.
    class DelayedEvaluation < GenericExpression
      # A callable object that returns the value of the evaluation
      # when called. 
      attr_reader :callable

      # Set the callable object
      def initialize(callable)
        @callable = callable
      end

      # Call the underlying callable and return the result.  If the
      # underlying callable only accepts a single argument, call it
      # with the given dataset.
      def call(ds)
        if @callable.respond_to?(:arity) && @callable.arity == 1
          @callable.call(ds)
        else
          @callable.call
        end
      end

      to_s_method :delayed_evaluation_sql
    end

    # Represents an SQL function call.
    class Function < GenericExpression
      WILDCARD = LiteralString.new('*').freeze
      DISTINCT = ["DISTINCT ".freeze].freeze
      COMMA_ARRAY = [LiteralString.new(', ').freeze].freeze

      # The SQL function to call
      attr_reader :name
      alias f name
      
      # The array of arguments to pass to the function (may be blank)
      attr_reader :args

      # Options for this function
      attr_reader :opts

      # Set the name and args for the function
      def initialize(name, *args)
        @name = name
        @args = args
        @opts = OPTS
      end

      def self.new!(name, args, opts)
        f = new(name, *args)
        f.instance_variable_set(:@opts, opts)
        f
      end

      # If no arguments are given, return a new function with the wildcard prepended to the arguments.
      #
      #   Sequel.function(:count).*  # count(*)
      def *(ce=(arg=false;nil))
        if arg == false
          raise Error, "Cannot apply * to functions with arguments" unless args.empty?
          with_opts(:"*"=>true)
        else
          super(ce)
        end
      end

      # Return a new function with DISTINCT before the method arguments.
      #
      #   Sequel.function(:count, :col).distinct # count(DISTINCT col)
      def distinct
        with_opts(:distinct=>true)
      end

      # Return a new function with FILTER added to it, for filtered
      # aggregate functions:
      #
      #   Sequel.function(:foo, :col).filter(:a=>1) # foo(col) FILTER (WHERE a = 1)
      def filter(*args, &block)
        args = args.first if args.length == 1
        with_opts(:filter=>args, :filter_block=>block)
      end

      # Return a function which will use LATERAL when literalized:
      #
      #   Sequel.function(:foo, :col).lateral # LATERAL foo(col)
      def lateral
        with_opts(:lateral=>true)
      end

      # Return a new function with an OVER clause (making it a window function).
      #
      #   Sequel.function(:row_number).over(:partition=>:col) # row_number() OVER (PARTITION BY col)
      def over(window=OPTS)
        raise Error, "function already has a window applied to it" if opts[:over]
        window = Window.new(window) unless window.is_a?(Window)
        with_opts(:over=>window)
      end

      # Return a new function where the function name will be quoted if the database supports
      # quoted functions:
      #
      #   Sequel.function(:foo).quoted # "foo"()
      def quoted
        with_opts(:quoted=>true)
      end

      # Return a new function where the function name will not be quoted even
      # if the database supports quoted functions:
      #
      #   Sequel.expr(:foo).function.unquoted # foo()
      def unquoted
        with_opts(:quoted=>false)
      end

      # Return a new function that will use WITH ORDINALITY to also return
      # a row number for every row the function returns:
      #
      #   Sequel.function(:foo).with_ordinality # foo() WITH ORDINALITY
      def with_ordinality
        with_opts(:with_ordinality=>true)
      end

      # Return a new function that uses WITHIN GROUP ordered by the given expression,
      # useful for ordered-set and hypothetical-set aggregate functions:
      #
      #   Sequel.function(:rank, :a).within_group(:b, :c)
      #   # rank(a) WITHIN GROUP (ORDER BY b, c)
      def within_group(*expressions)
        with_opts(:within_group=>expressions)
      end

      to_s_method :function_sql

      private

      # Return a new function call with the given opts merged into the current opts.
      def with_opts(opts)
        self.class.new!(name, args, @opts.merge(opts))
      end
    end

    class GenericExpression
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

    # Represents an identifier (column or table). Can be used
    # to specify a +Symbol+ with multiple underscores should not be
    # split, or for creating an identifier without using a symbol.
    class Identifier < GenericExpression
      include QualifyingMethods

      # The table or column to reference
      attr_reader :value

      # Set the value to the given argument
      def initialize(value)
        @value = value
      end

      # Create a Function using this identifier as the functions name, with
      # the given args.
      def function(*args)
        Function.new(self, *args)
      end
      
      to_s_method :quote_identifier, '@value'
    end
    
    # Represents an SQL JOIN clause, used for joining tables.
    class JoinClause < Expression
      # The type of join to do
      attr_reader :join_type

      # The expression representing the table/set related to the JOIN.
      # Is an AliasedExpression if the JOIN uses an alias.
      attr_reader :table_expr

      # Create an object with the given join_type and table expression.
      def initialize(join_type, table_expr)
        @join_type = join_type
        @table_expr = table_expr
      end

      # The table/set related to the JOIN, without any alias.
      def table
        if @table_expr.is_a?(AliasedExpression)
          @table_expr.expression
        else
          @table_expr
        end
      end

      # The table alias to use for the JOIN , or nil if the
      # JOIN does not alias the table.
      def table_alias
        if @table_expr.is_a?(AliasedExpression)
          @table_expr.alias
        end
      end

      # The column aliases to use for the JOIN , or nil if the
      # JOIN does not use a derived column list.
      def column_aliases
        if @table_expr.is_a?(AliasedExpression)
          @table_expr.columns
        end
      end

      to_s_method :join_clause_sql
    end

    # Represents an SQL JOIN clause with ON conditions.
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

    # Represents an SQL JOIN clause with USING conditions.
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
    # required for the prepared statement support and for database-specific
    # literalization.
    class PlaceholderLiteralString < GenericExpression
      # The literal string containing placeholders.  This can also be an array
      # of strings, where each arg in args goes between the string elements. 
      attr_reader :str

      # The arguments that will be subsituted into the placeholders.
      # Either an array of unnamed placeholders (which will be substituted in
      # order for ? characters), or a hash of named placeholders (which will be
      # substituted for :key phrases).
      attr_reader :args

      # Whether to surround the expression with parantheses
      attr_reader :parens

      # Create an object with the given string, placeholder arguments, and parens flag.
      def initialize(str, args, parens=false)
        @str = str
        @args = args.is_a?(Array) && args.length == 1 && (v = args.at(0)).is_a?(Hash) ? v : args
        @parens = parens
      end

      to_s_method :placeholder_literal_string_sql
    end

    # Subclass of +ComplexExpression+ where the expression results
    # in a numeric value in SQL.
    class NumericExpression < ComplexExpression
      include BitwiseMethods 
      include NumericMethods
      include InequalityMethods

      # Always use + for + operator for NumericExpressions.
      def +(ce)
        NumericExpression.new(:+, self, ce)
      end

      # Return self instead of creating a new object to save on memory.
      def sql_number
        self
      end
    end

    # Represents a column/expression to order the result set by.
    class OrderedExpression < Expression
      INVERT_NULLS = {:first=>:last, :last=>:first}.freeze

      # The expression to order the result set by.
      attr_reader :expression

      # Whether the expression should order the result set in a descending manner
      attr_reader :descending

      # Whether to sort NULLS FIRST/LAST
      attr_reader :nulls

      # Set the expression and descending attributes to the given values.
      # Options:
      #
      # :nulls :: Can be :first/:last for NULLS FIRST/LAST.
      def initialize(expression, descending = true, opts=OPTS)
        @expression, @descending, @nulls = expression, descending, opts[:nulls]
      end

      # Return a copy that is ordered ASC
      def asc
        OrderedExpression.new(@expression, false, :nulls=>@nulls)
      end

      # Return a copy that is ordered DESC
      def desc
        OrderedExpression.new(@expression, true, :nulls=>@nulls)
      end

      # Return an inverted expression, changing ASC to DESC and NULLS FIRST to NULLS LAST.
      def invert
        OrderedExpression.new(@expression, !@descending, :nulls=>INVERT_NULLS.fetch(@nulls, @nulls))
      end

      to_s_method :ordered_expression_sql
    end

    # Represents a qualified identifier (column with table or table with schema).
    class QualifiedIdentifier < GenericExpression
      include QualifyingMethods

      # The table/schema qualifying the reference
      attr_reader :table

      # The column/table referenced
      attr_reader :column

      # Set the table and column to the given arguments
      def initialize(table, column)
        @table, @column = table, column
      end
      
      # Create a Function using this identifier as the functions name, with
      # the given args.
      def function(*args)
        Function.new(self, *args)
      end
      
      to_s_method :qualified_identifier_sql, "@table, @column"
    end
    
    # Subclass of +ComplexExpression+ where the expression results
    # in a text/string/varchar value in SQL.
    class StringExpression < ComplexExpression
      include StringMethods
      include StringConcatenationMethods
      include InequalityMethods

      # Map of [regexp, case_insenstive] to +ComplexExpression+ operator symbol
      LIKE_MAP = {[true, true]=>:'~*', [true, false]=>:~, [false, true]=>:ILIKE, [false, false]=>:LIKE}
      
      # Creates a SQL pattern match exprssion. left (l) is the SQL string we
      # are matching against, and ces are the patterns we are matching.
      # The match succeeds if any of the patterns match (SQL OR).
      #
      # If a regular expression is used as a pattern, an SQL regular expression will be
      # used, which is currently only supported on MySQL and PostgreSQL.  Be aware
      # that MySQL and PostgreSQL regular expression syntax is similar to ruby
      # regular expression syntax, but it not exactly the same, especially for
      # advanced regular expression features.  Sequel just uses the source of the
      # ruby regular expression verbatim as the SQL regular expression string.
      #
      # If any other object is used as a regular expression, the SQL LIKE operator will
      # be used, and should be supported by most databases.  
      # 
      # The pattern match will be case insensitive if the last argument is a hash
      # with a key of :case_insensitive that is not false or nil. Also,
      # if a case insensitive regular expression is used (//i), that particular
      # pattern which will always be case insensitive.
      #
      #   StringExpression.like(:a, 'a%') # "a" LIKE 'a%' ESCAPE '\'
      #   StringExpression.like(:a, 'a%', :case_insensitive=>true) # "a" ILIKE 'a%' ESCAPE '\'
      #   StringExpression.like(:a, 'a%', /^a/i) # "a" LIKE 'a%' ESCAPE '\' OR "a" ~* '^a'
      def self.like(l, *ces)
        l, lre, lci = like_element(l)
        lci = (ces.last.is_a?(Hash) ? ces.pop : {})[:case_insensitive] ? true : lci
        ces.collect! do |ce|
          r, rre, rci = like_element(ce)
          BooleanExpression.new(LIKE_MAP[[lre||rre, lci||rci]], l, r)
        end
        ces.length == 1 ? ces.at(0) : BooleanExpression.new(:OR, *ces)
      end
      
      # Returns a three element array, made up of:
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

      # Return self instead of creating a new object to save on memory.
      def sql_string
        self
      end
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

      # Create a new +Subscript+ appending the given subscript(s)
      # to the current array of subscripts.
      #
      #   :a.sql_subscript(2) # a[2]
      #   :a.sql_subscript(2) | 1 # a[2, 1]
      def |(sub)
        Subscript.new(@f, @sub + Array(sub))
      end

      # Create a new +Subscript+ by accessing a subarray of a multidimensional
      # array.
      #
      #   :a.sql_subscript(2) # a[2]
      #   :a.sql_subscript(2)[1] # a[2][1]
      def [](sub)
        Subscript.new(self, Array(sub))
      end
      
      to_s_method :subscript_sql
    end

    # Represents an SQL value list (IN/NOT IN predicate value).  Added so it is possible to deal with a
    # ruby array of two element arrays as an SQL value list instead of an ordered
    # hash-like conditions specifier.
    class ValueList < ::Array
    end

    # The purpose of the +VirtualRow+ class is to allow the easy creation of SQL identifiers and functions
    # without relying on methods defined on +Symbol+.  This is useful if another library defines
    # the methods defined by Sequel, if you are running on ruby 1.9, or if you are not using the
    # core extensions.
    #
    # An instance of this class is yielded to the block supplied to <tt>Dataset#filter</tt>, <tt>Dataset#order</tt>, and <tt>Dataset#select</tt>
    # (and the other methods that accept a block and pass it to one of those methods).
    # If the block doesn't take an argument, the block is instance_execed in the context of
    # an instance of this class.
    #
    # +VirtualRow+ uses +method_missing+ to return either an +Identifier+, +QualifiedIdentifier+, or +Function+
    # depending on how it is called.
    #
    # If a block is _not_ given, creates one of the following objects:
    #
    # +Function+ :: Returned if any arguments are supplied, using the method name
    #               as the function name, and the arguments as the function arguments.
    # +QualifiedIdentifier+ :: Returned if the method name contains __, with the
    #                          table being the part before __, and the column being the part after.
    # +Identifier+ :: Returned otherwise, using the method name.
    #
    # If a block is given, it returns a +Function+.  Note that the block is currently not called by the code, though
    # this may change in a future version.  If the first argument is:
    #
    # no arguments given :: creates a +Function+ with no arguments.
    # :* :: creates a +Function+ with a literal wildcard argument (*), mostly useful for COUNT.
    # :distinct :: creates a +Function+ that prepends DISTINCT to the rest of the arguments, mostly
    #              useful for aggregate functions.
    # :over :: creates a +Function+ with a window.  If a second argument is provided, it should be a hash
    #          of options which are used to create the +Window+ (with possible keys :window, :partition, :order, and :frame).  The
    #          arguments to the function itself should be specified as <tt>:*=>true</tt> for a wildcard, or via
    #          the <tt>:args</tt> option.
    #
    # Examples:
    #
    #   ds = DB[:t]
    #
    #   # Argument yielded to block
    #   ds.filter{|r| r.name < 2} # SELECT * FROM t WHERE (name < 2)
    #
    #   # Block without argument (instance_eval)
    #   ds.filter{name < 2} # SELECT * FROM t WHERE (name < 2)
    #
    #   # Qualified identifiers
    #   ds.filter{table__column + 1 < 2} # SELECT * FROM t WHERE ((table.column + 1) < 2)
    #
    #   # Functions
    #   ds.filter{is_active(1, 'arg2')} # SELECT * FROM t WHERE is_active(1, 'arg2')
    #   ds.select{version{}} # SELECT version() FROM t
    #   ds.select{count(:*){}} # SELECT count(*) FROM t
    #   ds.select{count(:distinct, col1){}} # SELECT count(DISTINCT col1) FROM t
    #
    #   # Window Functions
    #   ds.select{rank(:over){}} # SELECT rank() OVER () FROM t
    #   ds.select{count(:over, :*=>true){}} # SELECT count(*) OVER () FROM t
    #   ds.select{sum(:over, :args=>col1, :partition=>col2, :order=>col3){}} # SELECT sum(col1) OVER (PARTITION BY col2 ORDER BY col3) FROM t
    #
    #   # Math Operators
    #   ds.select{|o| o.+(1, :a).as(:b)} # SELECT (1 + a) AS b FROM t
    #   ds.select{|o| o.-(2, :a).as(:b)} # SELECT (2 - a) AS b FROM t
    #   ds.select{|o| o.*(3, :a).as(:b)} # SELECT (3 * a) AS b FROM t
    #   ds.select{|o| o./(4, :a).as(:b)} # SELECT (4 / a) AS b FROM t
    #
    #   # Boolean Operators
    #   ds.filter{|o| o.&({:a=>1}, :b)}    # SELECT * FROM t WHERE ((a = 1) AND b)
    #   ds.filter{|o| o.|({:a=>1}, :b)}    # SELECT * FROM t WHERE ((a = 1) OR b)
    #   ds.filter{|o| o.~({:a=>1})}        # SELECT * FROM t WHERE (a != 1)
    #   ds.filter{|o| o.~({:a=>1, :b=>2})} # SELECT * FROM t WHERE ((a != 1) OR (b != 2))
    #
    #   # Inequality Operators
    #   ds.filter{|o| o.>(1, :a)}  # SELECT * FROM t WHERE (1 > a)
    #   ds.filter{|o| o.<(2, :a)}  # SELECT * FROM t WHERE (2 < a)
    #   ds.filter{|o| o.>=(3, :a)} # SELECT * FROM t WHERE (3 >= a)
    #   ds.filter{|o| o.<=(4, :a)} # SELECT * FROM t WHERE (4 <= a)
    #
    #   # Literal Strings
    #   ds.filter{{a=>`some SQL`}} # SELECT * FROM t WHERE (a = some SQL)
    #
    # For a more detailed explanation, see the {Virtual Rows guide}[rdoc-ref:doc/virtual_rows.rdoc].
    class VirtualRow < BasicObject
      QUESTION_MARK = LiteralString.new('?').freeze
      DOUBLE_UNDERSCORE = '__'.freeze

      include OperatorBuilders

      %w'> < >= <='.each do |op|
        class_eval(<<-END, __FILE__, __LINE__ + 1)
          def #{op}(*args)
            SQL::BooleanExpression.new(:#{op}, *args)
          end
        END
      end

      # Return a literal string created with the given string.
      def `(s)
        Sequel::LiteralString.new(s)
      end

      # Return an +Identifier+, +QualifiedIdentifier+, or +Function+, depending
      # on arguments and whether a block is provided.  Does not currently call the block.
      # See the class level documentation.
      def method_missing(m, *args, &block)
        if block
          if args.empty?
            Function.new(m)
          else
            case args.shift
            when :*
              Function.new(m, *args).*
            when :distinct
              Function.new(m, *args).distinct
            when :over
              opts = args.shift || OPTS
              f = Function.new(m, *::Kernel.Array(opts[:args]))
              f = f.* if opts[:*]
              f.over(opts)
            else
              Kernel.raise(Error, 'unsupported VirtualRow method argument used with block')
            end
          end
        elsif args.empty?
          table, column = m.to_s.split(DOUBLE_UNDERSCORE, 2)
          column ? QualifiedIdentifier.new(table, column) : Identifier.new(m)
        else
          Function.new(m, *args)
        end
      end

      Sequel::VIRTUAL_ROW = new
    end

    # A +Window+ is part of a window function specifying the window over which a window function operates.
    class Window < Expression
      # The options for this window.  Options currently supported:
      # :frame :: if specified, should be :all, :rows, or a String that is used literally. :all always operates over all rows in the
      #           partition, while :rows excludes the current row's later peers.  The default is to include
      #           all previous rows in the partition up to the current row's last peer.
      # :order :: order on the column(s) given
      # :partition :: partition/group on the column(s) given
      # :window :: base results on a previously specified named window
      attr_reader :opts

      # Set the options to the options given
      def initialize(opts=OPTS)
        @opts = opts
      end

      to_s_method :window_sql, '@opts'
    end

    # A +Wrapper+ is a simple way to wrap an existing object so that it supports
    # the Sequel DSL.
    class Wrapper < GenericExpression
      # The underlying value wrapped by this object.
      attr_reader :value

      # Set the value wrapped by the object.
      def initialize(value)
        @value = value
      end

      to_s_method :literal, '@value'
    end
  end

  # +LiteralString+ is used to represent literal SQL expressions. A 
  # +LiteralString+ is copied verbatim into an SQL statement. Instances of
  # +LiteralString+ can be created by calling <tt>Sequel.lit</tt>.
  class LiteralString
    include SQL::OrderMethods
    include SQL::ComplexExpressionMethods
    include SQL::BooleanMethods
    include SQL::NumericMethods
    include SQL::StringMethods
    include SQL::InequalityMethods
    include SQL::AliasMethods
    include SQL::CastMethods
      
    # Return self if no args are given, otherwise return a SQL::PlaceholderLiteralString
    # with the current string and the given args.
    def lit(*args)
      args.empty? ? self : SQL::PlaceholderLiteralString.new(self, args)
    end
      
    # Convert a literal string to a SQL::Blob.
    def to_sequel_blob
      SQL::Blob.new(self)
    end
  end
  
  include SQL::Constants
  extend SQL::Builders
  extend SQL::OperatorBuilders
end
