class Sequel::Dataset
  # The Sequelizer module includes methods for translating Ruby expressions
  # into SQL expressions, making it possible to specify dataset filters using
  # blocks, e.g.:
  #
  #   DB[:items].filter {:price < 100}
  #   DB[:items].filter {:category == 'ruby' && :date < 3.days.ago}
  #
  # Block filters can refer to literals, variables, constants, arguments, 
  # instance variables or anything else in order to create parameterized 
  # queries. Block filters can also refer to other dataset objects as 
  # sub-queries. Block filters are pretty much limitless!
  #
  # Block filters are based on ParseTree. If you do not have the ParseTree
  # gem installed, block filters will raise an error.
  #
  # To enable full block filter support make sure you have both ParseTree and
  # Ruby2Ruby installed:
  #
  #   sudo gem install parsetree
  #   sudo gem install ruby2ruby
  module Sequelizer
    # Formats an comparison expression involving a left value and a right
    # value. Comparison expressions differ according to the class of the right
    # value. The stock implementation supports Range (inclusive and exclusive),
    # Array (as a list of values to compare against), Dataset (as a subquery to
    # compare against), or a regular value.
    #
    #   dataset.compare_expr('id', 1..20) #=>
    #     "(id >= 1 AND id <= 20)"
    #   dataset.compare_expr('id', [3,6,10]) #=>
    #     "(id IN (3, 6, 10))"
    #   dataset.compare_expr('id', DB[:items].select(:id)) #=>
    #     "(id IN (SELECT id FROM items))"
    #   dataset.compare_expr('id', nil) #=>
    #     "(id IS NULL)"
    #   dataset.compare_expr('id', 3) #=>
    #     "(id = 3)"
    def compare_expr(l, r)
      case r
      when Range:
        r.exclude_end? ? \
          "(#{l} >= #{literal(r.begin)} AND #{l} < #{literal(r.end)})" : \
          "(#{l} >= #{literal(r.begin)} AND #{l} <= #{literal(r.end)})"
      when Array:
        "(#{literal(l)} IN (#{literal(r)}))"
      when Sequel::Dataset:
        "(#{literal(l)} IN (#{r.sql}))"
      when NilClass:
        "(#{literal(l)} IS NULL)"
      when Regexp:
        match_expr(l, r)
      else
        "(#{literal(l)} = #{literal(r)})"
      end
    end
    
    # Formats a string matching expression. The stock implementation supports
    # matching against strings only using the LIKE operator. Specific adapters
    # can override this method to provide support for regular expressions.
    def match_expr(l, r)
      case r
      when String:
        "(#{literal(l)} LIKE #{literal(r)})"
      else
        raise SequelError, "Unsupported match pattern class (#{r.class})."
      end
    end

    # Evaluates a method call. This method is used to evaluate Ruby expressions
    # referring to indirect values, e.g.:
    #
    #   dataset.filter {:category => category.to_s}
    #   dataset.filter {:x > y[0..3]}
    #
    # This method depends on the Ruby2Ruby gem. If you do not have Ruby2Ruby 
    # installed, this method will raise an error.
    def ext_expr(e, b)
      eval(RubyToRuby.new.process(e), b)
    end

    # Translates a method call parse-tree to SQL expression. The following 
    # operators are recognized and translated to SQL expressions: >, <, >=, <=,
    # ==, =~, +, -, *, /, %:
    #
    #   :x == 1 #=> "(x = 1)"
    #   (:x + 100) < 200 #=> "((x + 100) < 200)"
    #
    # The in, in?, nil and nil? method calls are intercepted and passed to 
    # #compare_expr.
    #
    #   :x.in [1, 2, 3] #=> "(x IN (1, 2, 3))"
    #   :x.in?(DB[:y].select(:z)) #=> "(x IN (SELECT z FROM y))"
    #   :x.nil? #=> "(x IS NULL)"
    #
    # The like and like? method calls are intercepted and passed to #match_expr.
    #
    #   :x.like? 'ABC%' #=> "(x LIKE 'ABC%')"
    #
    # The method also supports SQL functions by invoking Symbol#[]:
    #
    #   :avg[:x] #=> "avg(x)"
    #   :substring[:x, 5] #=> "substring(x, 5)"
    #
    # All other method calls are evaulated as normal Ruby code.
    def call_expr(e, b)
      case op = e[2]
      when :>, :<, :>=, :<=
        l = eval_expr(e[1], b)
        r = eval_expr(e[3][1], b)
        "(#{literal(l)} #{op} #{literal(r)})"
      when :==
        l = eval_expr(e[1], b)
        r = eval_expr(e[3][1], b)
        compare_expr(l, r)
      when :=~
        l = eval_expr(e[1], b)
        r = eval_expr(e[3][1], b)
        match_expr(l, r)
      when :+, :-, :*, :/, :%
        l = eval_expr(e[1], b)
        r = eval_expr(e[3][1], b)
        "(#{literal(l)} #{op} #{literal(r)})".lit
      when :in, :in?
        # in/in? operators are supported using two forms:
        #   :x.in([1, 2, 3])
        #   :x.in(1, 2, 3) # variable arity
        l = eval_expr(e[1], b)
        r = eval_expr((e[3].size == 2) ? e[3][1] : e[3], b)
        compare_expr(l, r)
      when :nil, :nil?
        l = eval_expr(e[1], b)
        compare_expr(l, nil)
      when :like, :like?
        l = eval_expr(e[1], b)
        r = eval_expr(e[3][1], b)
        match_expr(l, r)
      else
        if (op == :[]) && (e[1][0] == :lit) && (Symbol === e[1][1])
          # SQL Functions, e.g.: :sum[:x]
          e[1][1][*pt_expr(e[3], b)]
        else
          # external code
          ext_expr(e, b)
        end
      end
    end
    
    # Evaluates a parse-tree into an SQL expression.
    def eval_expr(e, b)
      case e[0]
      when :call # method call
        call_expr(e, b)
      when :ivar, :cvar, :dvar, :vcall, :const, :gvar # local ref
        eval(e[1].to_s, b)
      when :nth_ref:
        eval("$#{e[1]}", b)
      when :lvar: # local context
        if e[1] == :block
          pr = eval(e[1].to_s, b)
          "#{proc_to_sql(pr)}"
        else
          eval(e[1].to_s, b)
        end
      when :lit, :str # literal
         e[1]
      when :dot2 # inclusive range
        eval_expr(e[1], b)..eval_expr(e[2], b)
      when :dot3 # exclusive range
        eval_expr(e[1], b)...eval_expr(e[2], b)
      when :colon2 # qualified constant ref
        eval_expr(e[1], b).const_get(e[2])
      when :false: false
      when :true: true
      when :nil: nil
      when :array
        # array
        e[1..-1].map {|i| eval_expr(i, b)}
      when :match3
        # =~/!~ operator
        l = eval_expr(e[2], b)
        r = eval_expr(e[1], b)
        compare_expr(l, r)
      when :iter
        if e[1] == [:fcall, :proc]
          eval_expr(e[3], b) # inline proc
        else
          ext_expr(e, b) # method call with inline proc
        end
      when :dasgn, :dasgn_curr
        # assignment
        l = e[1]
        r = eval_expr(e[2], b)
        raise SequelError, "Invalid expression #{l} = #{r}. Did you mean :#{l} == #{r}?"
      else
        raise SequelError, "Invalid expression tree: #{e.inspect}"
      end
    end
    
    def pt_expr(e, b)
      case e[0]
      when :not # negation: !x, (x != y), (x !~ y)
        if (e[1][0] == :lit) && (Symbol === e[1][1])
          # translate (!:x) into (x = 'f')
          compare_expr(e[1][1], false)
        else
          "(NOT #{pt_expr(e[1], b)})"
        end
      when :block, :and # block of statements, x && y
        "(#{e[1..-1].map {|i| pt_expr(i, b)}.join(" AND ")})"
      when :or # x || y
        "(#{pt_expr(e[1], b)} OR #{pt_expr(e[2], b)})"
      when :call, :vcall, :iter # method calls, blocks
        eval_expr(e, b)
      else # literals
        if e == [:lvar, :block]
          eval_expr(e, b)
        else
          literal(eval_expr(e, b))
        end
      end
    end

    # Translates a Ruby block into an SQL expression.
    def proc_to_sql(proc)
      c = Class.new {define_method(:m, &proc)}
      pt_expr(ParseTree.translate(c, :m)[2][2], proc.binding)
    end
  end
end

begin
  require 'parse_tree'
rescue LoadError
  module Sequel::Dataset::Sequelizer
    def proc_to_sql(proc)
      raise SequelError, "You must have the ParseTree gem installed in order to use block filters."
    end
  end
end

begin
  require 'ruby2ruby'
rescue LoadError
  module Sequel::Dataset::Sequelizer
    def ext_expr(e)
      raise SequelError, "You must have the Ruby2Ruby gem installed in order to use this block filter."
    end
  end
end
