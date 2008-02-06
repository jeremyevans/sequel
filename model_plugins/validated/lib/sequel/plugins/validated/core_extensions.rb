require File.dirname(__FILE__) + '/inflector'
require File.dirname(__FILE__) + '/time_calculations'

module Kernel
  # Ruby 1.8 compatibility
  alias_method :send!, :send unless methods.include? 'send!'
end

class Module
  def alias_method_chain(method, feature)
    alias_method :"#{method}_without_#{feature}", method
    alias_method method, :"#{method}_with_#{feature}"
  end
end

class Array
  alias_method :blank?, :empty?
  def extract_options
    if last.is_a? Hash then last else {} end
  end
  def extract_options!
    if last.is_a? Hash then pop else {} end
  end
end

class Object
  def blank?
    nil? || (respond_to?(:empty?) && empty?)
  end
end

class Numeric
  include Sequel::Plugins::Validated::TimeCalculations
  def blank?
    false
  end
end

class NilClass
  alias_method :blank?, :nil?
end

class TrueClass
  def blank?
    false
  end
end

class FalseClass
  def blank?
    true
  end
end

class String
  include Sequel::Plugins::Validated::Inflections
  def blank?
    strip.empty?
  end
end
