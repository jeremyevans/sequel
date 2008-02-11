require 'delegate'
require 'observer'

require 'rubygems'
require 'assistance'

module Kernel#:nodoc:all
  methods.include? 'send!' or
  alias_method :send!, :send
end

$:.unshift File.dirname(__FILE__)

module NotNaughty
  require 'not_naughty/validator'

  require 'not_naughty/builder'
  require 'not_naughty/validation'
  Validation.add_observer Builder
  
  require 'not_naughty/errors'
  require 'not_naughty/instance_methods'
  
  # Extended classes get NotNaughty::Builder and NotNaughty::InstanceMethods.
  def self.extended(base)
    base.instance_eval do
      include InstanceMethods
      extend Builder
    end
  end
  
  # call-seq:
  # validator(validator_klass = NotNaughty::Validator, *states = [:default])
  #
  # Returns instance of Validator. This is either the validator's clone of
  # superclass, an instance of the the given descendant of or the
  # <tt>NotNaughty:Validator</tt> himself.
  #
  # <b>Examples:</b>
  #   validator # => Instance of NotNaughty::Validator with :default state
  #   validator :create, :update # ~ - but with :create and :update states
  #   validator AnotherValidator # Instance of AnotherValidator
  #
  # The above examples work as long validator is not already called. To reset
  # an already assigned validator set <tt>@validator</tt> to nil.
  def validator(*states)
    @validator ||= if superclass.respond_to? :validator
      superclass.validator.clone

    else
      validator_klass =
        if states[0].is_a? Class and states[0] <= NotNaughty::Validator
          states.shift
        else
          NotNaughty::Validator
        end
      
      validator_klass.new(*states)
    end
  end
  
  # Prepends a call for validation before then given method. If, on call, the
  # validation passes the method is called. Otherwise it raises an
  # NotNaughty::ValidationException or returns false.
  #
  # <b>Example:</b>
  #   validated_before :save # raise ValidationException unless valid?
  #   validated_before :save, :without => :exception # => false unless valid?
  def validated_before(method, *args)
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
