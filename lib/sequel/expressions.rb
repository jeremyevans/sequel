# Based on great work by Sam Smoot
# http://substantiality.net/archives/2007/4/16/datamapper-part-xiv-finder-block

module Sequel
  class Dataset
    class BlankSlate #:nodoc:
      instance_methods.each { |m| undef_method m unless m =~ /^(__|instance_eval)/ }
    end

    class Expression < BlankSlate
      attr_reader :left, :right
      attr_accessor :op

      def initialize(left)
        @left = left
      end

      def method_missing(sym, *right)
        @op = case sym
          when :==, :===, :in: :eql
          when :=~: :like
          when :"<=>": :not
          when :<: :lt
          when :<=: :lte
          when :>: :gt
          when :>=: :gte
          else sym
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

    class ExpressionCompiler < BlankSlate
      def initialize(&block)
        @block = block
        @expressions = []
      end

      def method_missing(sym, *args)
        expr = Expression.new(sym)
        @expressions << expr
        expr
      end
      
      def SUM(sym)
        expr = Expression.new(sym.SUM)
        @expressions << expr
        expr
      end
      
      def __to_a__
        instance_eval(&@block)
        @expressions
      end
    end
  end
end

class Proc
  def to_expressions
    Sequel::Dataset::ExpressionCompiler.new(&self).__to_a__
  end
end
