module Sequel
  class Dataset
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
      when Range
        r.exclude_end? ? \
          "(#{literal(l)} >= #{literal(r.begin)} AND #{literal(l)} < #{literal(r.end)})" : \
          "(#{literal(l)} >= #{literal(r.begin)} AND #{literal(l)} <= #{literal(r.end)})"
      when Array
        "(#{literal(l)} IN (#{literal(r)}))"
      when Sequel::Dataset
        "(#{literal(l)} IN (#{r.sql}))"
      when NilClass
        "(#{literal(l)} IS NULL)"
      when Regexp
        collate_match_expr(l, r)
      else
        "(#{literal(l)} = #{literal(r)})"
      end
    end

    # Formats a string matching expression with support for multiple choices.
    # For more information see #match_expr.
    def collate_match_expr(l, r)
      if r.is_a?(Array)
        "(#{r.map {|i| match_expr(l, i)}.join(' OR ')})"
      else
        match_expr(l, r)
      end
    end

    # Formats a string matching expression. The stock implementation supports
    # matching against strings only using the LIKE operator. Specific adapters
    # can override this method to provide support for regular expressions.
    def match_expr(l, r)
      case r
      when String
        "(#{literal(l)} LIKE #{literal(r)})"
      else
        raise Sequel::Error, "Unsupported match pattern class (#{r.class})."
      end
    end
  end
end

begin
  require 'parse_tree'
  require 'sequel_core/dataset/parse_tree_sequelizer'
  class Proc
    def to_sql(dataset, opts = {})
      dataset.send(:pt_expr, to_sexp[2], self.binding, opts)
    end
  end
  begin
    require 'ruby2ruby'
    class Sequel::Dataset
      # Evaluates a method call. This method is used to evaluate Ruby expressions
      # referring to indirect values, e.g.:
      #
      #   dataset.filter {:category => category.to_s}
      #   dataset.filter {:x > y[0..3]}
      #
      # This method depends on the Ruby2Ruby gem. If you do not have Ruby2Ruby
      # installed, this method will raise an error.
      def ext_expr(e, b, opts)
        eval(::RubyToRuby.new.process(e), b)
      end
    end
    class Proc
      remove_method :to_sexp
    end
  rescue LoadError
    class Sequel::Dataset
      def ext_expr(*args)
        raise Sequel::Error, "You must have the Ruby2Ruby gem installed in order to use this block filter."
      end
    end
  ensure
    class Proc
      # replacement for Proc#to_sexp as defined in ruby2ruby.
      # see also: http://rubyforge.org/tracker/index.php?func=detail&aid=18095&group_id=1513&atid=5921
      # The ruby2ruby implementation leaks memory, so we fix it.
      def to_sexp
        block = self
        c = Class.new {define_method(:m, &block)}
        ParseTree.translate(c, :m)[2]
      end
    end
  end
rescue LoadError
  class Proc
    def to_sql(*args)
      raise Sequel::Error, "You must have the ParseTree gem installed in order to use block filters."
    end
  end
end
