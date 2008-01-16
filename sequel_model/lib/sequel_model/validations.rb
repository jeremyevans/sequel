module Sequel
  module Validatable
    module ClassMethods
      def self.add_validation_method(name)
        class_eval("def validates_#{name}(*args, &b); validates(#{name.inspect}, *args, &b); end")
      end

      def validations
        @validations ||= []
      end

      def validates(*args, &block)
        if block && args.empty?
          return Validation::Generator.new(self, &block)
        end
        validations << Sequel::Validation[args.shift].new(*args, &block)
      end
      
      def has_validations?
        !validations.empty?
      end
      
      def before_validation_hooks
        @before_validation_hooks ||= []
      end
      
      def before_validation(&block)
        before_validation_hooks << block
      end
      
      def validate(o)
        before_validation_hooks.each {|b| o.instance_eval(&b)}
        validations.each do |v|
          unless v.valid?(o)
            o.errors << v.failed_message(o)
          end
        end
      end
    end

    def self.included(c)
      c.extend ClassMethods
    end
    
    attr_accessor :errors

    def validate
      @errors = []
      self.class.validate(self)
    end
    
    def valid?
      validate
      errors.empty?
    end
  end

  class Validation
    @@validation_classes = {}
    
    def self.validation_name
      to_s =~ /([^:]+)$/
      $1.underscore.to_sym
    end
    
    def self.inherited(c)
      name = c.validation_name
      @@validation_classes[name] = c
      Sequel::Validatable::ClassMethods.add_validation_method(name)
    end
    
    def self.[](name)
      @@validation_classes[name] || \
        (raise Sequel::Error, "Unknown validation #{name}")
    end
    
    def self.check_required_options(opts)
      return unless @required_options
      keys = opts.keys
      @required_options.each do |o|
        unless keys.include?(o)
          raise Sequel::Error, "Missing option #{o} for #{validation_name} validation"
        end
      end
    end
    
    def self.required_option(o)
      @required_options ||= []
      @required_options << o
      option(o)
    end
    
    def self.default_options
      @default_options || {}
    end
    
    def self.default(opts)
      @default_options ||= {}
      @default_options.merge!(opts)
    end
    
    def self.option(*names)
      names.each do |n|
        class_def(n) {@opts[n]}
      end
    end
    
    attr_reader :attribute, :opts, :block

    def initialize(attribute = nil, opts = {}, &block)
      if Hash === attribute
        opts = attribute
        attribute = nil
      end
      @attribute, @opts = attribute, self.class.default_options.merge(opts)
      @block = block || @opts[:logic]
      self.class.check_required_options(@opts)
    end
    
    def message(o)
      attribute ? "#{attribute} is invalid" : \
        "#{self.class.validation_name} validation failed"
    end
    
    def failed_message(o)
      @opts[:message] || message(o)
    end

    class Generator
      def initialize(receiver ,&block)
        @receiver = receiver
        instance_eval(&block)
      end

      def method_missing(*args)
        @receiver.validates *args
      end
    end
    
    class AcceptanceOf < Validation
      option :allow_nil, :accept
      default :allow_nil => true, :accept => '1'
    
      def valid?(o)
        v = o.send(attribute)
        (v.nil? && allow_nil) ? true : (v == accept)
      end
  
      def message(o)
        "#{attribute} must be accepted"
      end
    end
    
    class ConfirmationOf < Validation
      option :case_sensitive
      default :case_sensitive => true
      
      def valid?(o)
        v1 = o.send(attribute).to_s
        v2 = o.send(:"#{attribute}_confirmation").to_s
        case_sensitive ? (v1 == v2) : (v1.casecmp(v2) == 0)
      end
      
      def message(o)
        "#{attribute} must be confirmed"
      end
    end
    
    class FormatOf < Validation
      required_option :with
      
      def valid?(o)
        !(o.send(attribute).to_s =~ with).nil?
      end
    end
    
    class Each < Validation
      def valid?(o)
        o.instance_eval(&@block)
        true
      end
    end

    class LengthOf < Validation
      option :minimum, :maximum, :is, :within, :allow_nil
  
      def valid?(o)
        valid = true
        unless v = o.send(self.attribute)
          return true if allow_nil
          v = ''
        end
  
        valid &&= v.length <= maximum if maximum
        valid &&= v.length >= minimum if minimum
        valid &&= v.length == is if is
        valid &&= within.include?(v.length) if within
        valid
      end
    end
    
    class NumericalityOf < Validation
      option :only_integer
      
      NUMBER_RE = /^\d*\.{0,1}\d+$/
      INTEGER_RE = /\A[+-]?\d+\Z/
      
      
      def valid?(o)
        v = o.send(attribute).to_s
        v =~ (only_integer ? INTEGER_RE : NUMBER_RE)
      end
      
      def message(o)
        "#{attribute} must be a number"
      end
    end
    
    class PresenceOf < Validation
      def valid?(o)
        !o.send(attribute).blank?
      end
      
      def message(o)
        "#{attribute} must be present"
      end
    end
    
    class TrueFor < Validation
      def valid?(o)
        o.instance_eval(&block)
      end
    end
  end

  class Model
    include Validatable
    
    alias_method :save!, :save
    def save(*args)
      return false unless valid?
      save!(*args)
    end
  end
end
