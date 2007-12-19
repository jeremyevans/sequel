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
          "(#{literal(l)} >= #{literal(r.begin)} AND #{literal(l)} < #{literal(r.end)})" : \
          "(#{literal(l)} >= #{literal(r.begin)} AND #{literal(l)} <= #{literal(r.end)})"
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
        raise Sequel::Error::UnsupportedMatchPatternClass, r.class
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
    def ext_expr(e, b, opts)
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
    def call_expr(e, b, opts)
      case op = e[2]
      when :>, :<, :>=, :<=
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        if l.is_one_of?(Symbol, Sequel::LiteralString, Sequel::SQL::Expression) || \
          r.is_one_of?(Symbol, Sequel::LiteralString, Sequel::SQL::Expression)
          "(#{literal(l)} #{op} #{literal(r)})"
        else
          ext_expr(e, b, opts)
        end
      when :==
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        compare_expr(l, r)
      when :=~
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        match_expr(l, r)
      when :+, :-, :*, :%, :/
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        if l.is_one_of?(Symbol, Sequel::LiteralString, Sequel::SQL::Expression) || \
          r.is_one_of?(Symbol, Sequel::LiteralString, Sequel::SQL::Expression)
          "(#{literal(l)} #{op} #{literal(r)})".lit
        else
          ext_expr(e, b, opts)
        end
      when :<<
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        "#{literal(l)} = #{literal(r)}".lit
      when :|
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        if l.is_one_of?(Symbol, Sequel::SQL::Subscript)
          l|r
        elsif l.is_one_of?(Symbol, Sequel::LiteralString, Sequel::SQL::Expression) || \
          r.is_one_of?(Symbol, Sequel::LiteralString, Sequel::SQL::Expression)
          "(#{literal(l)} #{op} #{literal(r)})".lit
        else
          ext_expr(e, b, opts)
        end
      when :in, :in?
        # in/in? operators are supported using two forms:
        #   :x.in([1, 2, 3])
        #   :x.in(1, 2, 3) # variable arity
        l = eval_expr(e[1], b, opts)
        r = eval_expr((e[3].size == 2) ? e[3][1] : e[3], b, opts)
        compare_expr(l, r)
      when :nil, :nil?
        l = eval_expr(e[1], b, opts)
        compare_expr(l, nil)
      when :like, :like?
        l = eval_expr(e[1], b, opts)
        r = eval_expr(e[3][1], b, opts)
        match_expr(l, r)
      else
        if (op == :[]) && (e[1][0] == :lit) && (Symbol === e[1][1])
          # SQL Functions, e.g.: :sum[:x]
          if e[3]
            e[1][1][*eval_expr(e[3], b, opts)]
          else
            e[1][1][]
          end
        else
          # external code
          ext_expr(e, b, opts)
        end
      end
    end
    
    def fcall_expr(e, b, opts) #:nodoc:
      ext_expr(e, b, opts)
    end
    
    def vcall_expr(e, b, opts) #:nodoc:
      eval(e[1].to_s, b)
    end
    
    def iter_expr(e, b, opts) #:nodoc:
      if e[1][0] == :call && e[1][2] == :each
        unfold_each_expr(e, b, opts)
      elsif e[1] == [:fcall, :proc]
        eval_expr(e[3], b, opts) # inline proc
      else
        ext_expr(e, b, opts) # method call with inline proc
      end
    end
    
    def replace_dvars(a, values)
      a.map do |i|
        if i.is_a?(Array) && (i[0] == :dvar)
          if v = values[i[1]]
            value_to_parse_tree(v)
          else
            i
          end
        elsif Array === i
          replace_dvars(i, values)
        else
          i
        end
      end
    end
    
    def value_to_parse_tree(value)
      c = Class.new
      c.class_eval("def m; #{value.inspect}; end")
      ParseTree.translate(c, :m)[2][1][2]
    end
    
    def unfold_each_expr(e, b, opts) #:nodoc:
      source = eval_expr(e[1][1], b, opts)
      block_dvars = []
      if e[2][0] == :dasgn_curr
        block_dvars << e[2][1]
      elsif e[2][0] == :masgn
        e[2][1].each do |i|
          if i.is_a?(Array) && i[0] == :dasgn_curr
            block_dvars << i[1]
          end
        end
      end
      new_block = [:block]
      
      source.each do |*dvars|
        iter_values = (Array === dvars[0]) ? dvars[0] : dvars
        values = block_dvars.inject({}) {|m, i| m[i] = iter_values.shift; m}
        iter = replace_dvars(e[3], values)
        new_block << iter
      end
      
      pt_expr(new_block, b, opts)
    end
    
    # Evaluates a parse-tree into an SQL expression.
    def eval_expr(e, b, opts)
      case e[0]
      when :call # method call
        call_expr(e, b, opts)
      when :fcall
        fcall_expr(e, b, opts)
      when :vcall
        vcall_expr(e, b, opts)
      when :ivar, :cvar, :dvar, :const, :gvar # local ref
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
        eval_expr(e[1], b, opts)..eval_expr(e[2], b, opts)
      when :dot3 # exclusive range
        eval_expr(e[1], b, opts)...eval_expr(e[2], b, opts)
      when :colon2 # qualified constant ref
        eval_expr(e[1], b, opts).const_get(e[2])
      when :false: false
      when :true: true
      when :nil: nil
      when :array
        # array
        e[1..-1].map {|i| eval_expr(i, b, opts)}
      when :match3
        # =~/!~ operator
        l = eval_expr(e[2], b, opts)
        r = eval_expr(e[1], b, opts)
        compare_expr(l, r)
      when :iter
        iter_expr(e, b, opts)
      when :dasgn, :dasgn_curr
        # assignment
        l = e[1]
        r = eval_expr(e[2], b, opts)
        raise Sequel::Error::InvalidExpression, "#{l} = #{r}. Did you mean :#{l} == #{r}?"
      when :if, :dstr
        ext_expr(e, b, opts)
      else
        raise Sequel::Error::InvalidExpressionTree, e.inspect
      end
    end
    
    JOIN_AND = " AND ".freeze
    JOIN_COMMA = ", ".freeze
    
    def pt_expr(e, b, opts = {}) #:nodoc:
      case e[0]
      when :not # negation: !x, (x != y), (x !~ y)
        if (e[1][0] == :lit) && (Symbol === e[1][1])
          # translate (!:x) into (x = 'f')
          compare_expr(e[1][1], false)
        else
          "(NOT #{pt_expr(e[1], b, opts)})"
        end
      when :and # x && y
        "(#{e[1..-1].map {|i| pt_expr(i, b, opts)}.join(JOIN_AND)})"
      when :or # x || y
        "(#{pt_expr(e[1], b, opts)} OR #{pt_expr(e[2], b, opts)})"
      when :call, :vcall, :iter, :match3 # method calls, blocks
        eval_expr(e, b, opts)
      when :block # block of statements
        if opts[:comma_separated]
          "#{e[1..-1].map {|i| pt_expr(i, b, opts)}.join(JOIN_COMMA)}"
        else
          "(#{e[1..-1].map {|i| pt_expr(i, b, opts)}.join(JOIN_AND)})"
        end
      else # literals
        if e == [:lvar, :block]
          eval_expr(e, b, opts)
        else
          literal(eval_expr(e, b, opts))
        end
      end
    end

    # Translates a Ruby block into an SQL expression.
    def proc_to_sql(proc, opts = {})
      c = Class.new {define_method(:m, &proc)}
      pt_expr(ParseTree.translate(c, :m)[2][2], proc.binding, opts)
    end
  end
end

begin
  require 'parse_tree'
rescue Exception
  module Sequel::Dataset::Sequelizer
    def proc_to_sql(proc)
      raise Sequel::Error, "You must have the ParseTree gem installed in order to use block filters."
    end
  end
end

begin
  require 'ruby2ruby'
rescue Exception
  module Sequel::Dataset::Sequelizer
    def ext_expr(e)
      raise Sequel::Error, "You must have the Ruby2Ruby gem installed in order to use this block filter."
    end
  end
end
