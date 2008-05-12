module Sequel::Dataset::Sequelizer
end

begin
  require 'parse_tree'
  require 'sequel_core/dataset/_sequelizer'
  class Proc
    def to_sql(dataset, opts = {})
      dataset.send(:pt_expr, to_sexp[2], self.binding, opts)
    end
  end
  begin
    require 'ruby2ruby'
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
    module Sequel::Dataset::Sequelizer
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
    end
  rescue LoadError
    module Sequel::Dataset::Sequelizer
      def ext_expr(*args)
        raise Sequel::Error, "You must have the Ruby2Ruby gem installed in order to use this block filter."
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
