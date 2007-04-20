# Based on great work by Sam Smoot
# http://substantiality.net/archives/2007/4/16/datamapper-part-xiv-finder-block

module Sequel
  class Dataset
    class BlankSlate #:nodoc:
      instance_methods.each { |m| undef_method m unless m =~ /^(__|instance_eval)/ }
    end

    # An Expression is made of a left side, an operator, and a right side.
    class Expression < BlankSlate
      attr_reader :left, :right
      attr_accessor :op

      def initialize(left)
        @left = left
      end

      def method_missing(sym, *right)
        @op = case sym
        when :==, :===, :in, :in?: :eql
        when :=~, :like, :like?: :like
        when :"<=>", :is_not: :not
        when :<: :lt
        when :<=: :lte
        when :>: :gt
        when :>=: :gte
        else
          @left = "#{left}.#{sym}"
        end
        @right = right.first
        self
      end
      
      def nil?
        @op = :eql
        @right = nil
        self
      end
    end

    # An ExpressionCompiler takes a Proc object and compiles it
    # into an array of expressions using instance_eval magic.
    class ExpressionCompiler < BlankSlate
      def initialize(&block) #:nodoc:
        @block = block
        @expressions = []
      end
      
      # Converts the block into an array of expressions.
      def __to_a__
        instance_eval(&@block)
        @expressions
      rescue => e
        raise SequelError, e.message
      end

      private
        def __expr(sym) #:nodoc:
          expr = Expression.new(sym)
          @expressions << expr
          expr
        end

        def method_missing(sym, *args); __expr(sym); end #:nodoc:
        def test; __expr(:test); end #:nodoc:
        def SUM(sym); __expr(sym.SUM); end #:nodoc:
    end
  end
end

class Proc
  def to_expressions
    Sequel::Dataset::ExpressionCompiler.new(&self).__to_a__
  end
end
