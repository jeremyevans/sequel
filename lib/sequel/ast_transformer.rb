module Sequel
  # The +ASTTransformer+ class is designed to handle the abstract syntax trees
  # that Sequel uses internally and produce modified copies of them.  By itself
  # it only produces a straight copy.  It's designed to be subclassed and have
  # subclasses returned modified copies of the specific nodes that need to
  # be modified.
  class ASTTransformer
    # Return +obj+ or a potentially transformed version of it.
    def transform(obj)
      v(obj)
    end

    private

    # Recursive version that handles all of Sequel's internal object types
    # and produces copies of them.
    def v(o)
      case o
      when Symbol, Numeric, String, Class, TrueClass, FalseClass, NilClass
        o
      when Array
        o.map{|x| v(x)}
      when Hash
        h = {}
        o.each{|k, val| h[v(k)] = v(val)}
        h
      when SQL::ComplexExpression
        SQL::ComplexExpression.new(o.op, *v(o.args))
      when SQL::Identifier
        SQL::Identifier.new(v(o.value))
      when SQL::QualifiedIdentifier
        SQL::QualifiedIdentifier.new(v(o.table), v(o.column))
      when SQL::OrderedExpression
        SQL::OrderedExpression.new(v(o.expression), o.descending, :nulls=>o.nulls)
      when SQL::AliasedExpression
        SQL::AliasedExpression.new(v(o.expression), o.aliaz)
      when SQL::CaseExpression
        args = [v(o.conditions), v(o.default)]
        args << v(o.expression) if o.expression?
        SQL::CaseExpression.new(*args)
      when SQL::Cast
        SQL::Cast.new(v(o.expr), o.type)
      when SQL::Function
        SQL::Function.new(o.f, *v(o.args))
      when SQL::Subscript
        SQL::Subscript.new(v(o.f), v(o.sub))
      when SQL::WindowFunction
        SQL::WindowFunction.new(v(o.function), v(o.window))
      when SQL::Window
        opts = o.opts.dup
        opts[:partition] = v(opts[:partition]) if opts[:partition]
        opts[:order] = v(opts[:order]) if opts[:order]
        SQL::Window.new(opts)
      when SQL::PlaceholderLiteralString
        args = if o.args.is_a?(Hash)
          h = {}
          o.args.each{|k,val| h[k] = v(val)}
          h
        else
          v(o.args)
        end
        SQL::PlaceholderLiteralString.new(o.str, args, o.parens)
      when SQL::JoinOnClause
        SQL::JoinOnClause.new(v(o.on), o.join_type, v(o.table), v(o.table_alias))
      when SQL::JoinUsingClause
        SQL::JoinOnClause.new(v(o.using), o.join_type, v(o.table), v(o.table_alias))
      when SQL::JoinClause
        SQL::JoinOnClause.new(o.join_type, v(o.table), v(o.table_alias))
      else
        o
      end
    end
  end

  # Handles qualifying existing datasets, so that unqualified columns
  # in the dataset are qualified with a given table name.
  class Qualifier < ASTTransformer
    # Store the dataset to use as the basis for qualification, 
    # and the table used to qualify unqualified columns. 
    def initialize(ds, table)
      @ds = ds
      @table = table
    end

    private

    # Turn <tt>SQL::Identifier</tt>s and symbols that aren't implicitly
    # qualified into <tt>SQL::QualifiedIdentifier</tt>s.  For symbols that
    # are not implicitly qualified by are implicitly aliased, return an
    # <tt>SQL::AliasedExpression</tt>s with a qualified version of the symbol.
    def v(o)
      case o
      when Symbol
        t, column, aliaz = @ds.send(:split_symbol, o)
        if t
          o
        elsif aliaz
          SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(@table, SQL::Identifier.new(column)), aliaz)
        else
          SQL::QualifiedIdentifier.new(@table, o)
        end
      when SQL::Identifier
        SQL::QualifiedIdentifier.new(@table, o)
      when SQL::QualifiedIdentifier, SQL::JoinClause
        # Return these directly, so we don't accidentally qualify symbols in them.
        o
      else
        super
      end
    end
  end

  # +Unbinder+ is used to take a dataset filter and return a modified version
  # that unbinds already bound values and returns a dataset with bound value
  # placeholders and a hash of bind values.  You can then prepare the dataset
  # and use the bound variables to execute it with the same values.
  #
  # This class only does a limited form of unbinding where the variable names
  # and values can be associated unambiguously.  The only cases it handles
  # are <tt>SQL::ComplexExpression<tt> with an operator in +UNBIND_OPS+, a
  # first argument that's an instance of a member of +UNBIND_KEY_CLASSES+, and
  # a second argument that's an instance of a member of +UNBIND_VALUE_CLASSES+.
  #
  # So it can handle cases like:
  #
  #   DB.filter(:a=>1).exclude(:b=>2).where{c > 3}
  #
  # But it cannot handle cases like:
  #
  #   DB.filter(:a + 1 < 0)
  class Unbinder < ASTTransformer
    # The <tt>SQL::ComplexExpression<tt> operates that will be considered
    # for transformation.
    UNBIND_OPS = [:'=', :'!=', :<, :>, :<=, :>=]

    # The key classes (first argument of the ComplexExpression) that will
    # considered for transformation.
    UNBIND_KEY_CLASSES = [Symbol, SQL::Identifier, SQL::QualifiedIdentifier]

    # The value classes (second argument of the ComplexExpression) that
    # will be considered for transformation.
    UNBIND_VALUE_CLASSES = [Numeric, String, Date, Time]

    # The hash of bind variables that were extracted from the dataset filter.
    attr_reader :binds

    # Intialize an empty +binds+ hash.
    def initialize
      @binds = {}
    end

    private

    # Create a suitable bound variable key for the object, which should be
    # an instance of one of the +UNBIND_KEY_CLASSES+.
    def bind_key(obj)
      case obj
      when Symbol, String
        obj
      when SQL::Identifier
        bind_key(obj.value)
      when SQL::QualifiedIdentifier
        :"#{bind_key(obj.table)}.#{bind_key(obj.column)}"
      else
        raise Error, "unhandled object in Sequel::Unbinder#bind_key: #{obj}"
      end
    end

    # Handle <tt>SQL::ComplexExpression</tt> instances with suitable ops
    # and arguments, substituting the value with a bound variable placeholder
    # and assigning it an entry in the +binds+ hash with a matching key.
    def v(o)
      if o.is_a?(SQL::ComplexExpression) && UNBIND_OPS.include?(o.op)
        l, r = o.args
        if UNBIND_KEY_CLASSES.any?{|c| l.is_a?(c)} && UNBIND_VALUE_CLASSES.any?{|c| r.is_a?(c)} && !r.is_a?(LiteralString)
          key = bind_key(l)
          if (old = binds[key]) && old != r
            raise UnbindDuplicate, "two different values for #{key.inspect}: #{[r, old].inspect}" 
          end
          binds[key] = r
          SQL::ComplexExpression.new(o.op, l, :"$#{key}")
        else
          super
        end
      else
        super
      end
    end
  end
end
