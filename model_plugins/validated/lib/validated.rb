require 'rubygems'
require 'assistance'

module Kernel#:nodoc:all
  methods.include? 'send!' or
  alias_method :send!, :send
end

$:.unshift File.expand_path(File.join(File.dirname(__FILE__), %w[.. lib]))

module Validated
  require 'validated/validator'

  require 'validated/builder'
  require 'validated/validation'
  Validation.add_observer Builder
  
  require 'validated/errors'
  require 'validated/instance_methods'
  require 'validated/validations'
  
  def def_validator(validator_klass = Validated::Validator, *states)
    (class << self; self; end).module_eval do
      define_method :validator do
        @validator ||= if superclass.respond_to? :validator
          superclass.validator.clone
        else
          validator_klass.new(*states)
        end
      end
    end
    
    extend Builder
    include InstanceMethods
  end
  
  def def_validated_before(method, *args)
    __method = :"#{method}_without_validations"
    alias_method __method, method

    without = args.extract_options![:without]
    if [*without].include? :exception
      define_method method do |*params|
        if valid? then send! __method else false end
      end
    else
      define_method method do |*params|
        if valid? then send! __method else raise errors.to_exception end
      end
    end
  end
  
end
