module Sequel
  module Validatable
    module ClassMethods
      def self.add_validation_method(name)
        define_method("validates_#{name}".to_sym) {|*args| validates name, *args}
      end

      def validates(*args, &block)
        if block
          return Validation::Generator.new(self, &block)
        end
        @validations ||= []
        @validations << Sequel::Validation[args.shift].new(*args)
      end
      
      def validations
        @validations
      end
      
      def has_validations?
        @validations && !@validations.empty?
      end
    end

    def self.included(c)
      c.extend ClassMethods
    end
    
    def valid?
      @validation_errors = []
      self.class.validations.each do |v|
        unless v.valid?(self)
          @validation_errors << v.failed_message(self)
        end
      end
      @validation_errors.empty?
    end
    
    attr_reader :validation_errors
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
    
    attr_reader :attribute, :opts

    def initialize(attribute = nil, opts = {})
      if Hash === attribute
        opts = attribute
        attribute = nil
      end
      @attribute, @opts = attribute, self.class.default_options.merge(opts)
      self.class.check_required_options(@opts)
    end
    
    def failed_message(o)
      @opts[:message] || "#{self.class.validation_name} #{attribute} validation failed"
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
  end
end

__END__

require 'validatable'

module Sequel
  class Model
    # =Basic Sequel Validations
    #
    # Sequel validations are based on the Validatable gem http://validatable.rubyforge.org/
    # 
    # To assign default validations to a sequel model:
    # 
    # class MyModel < SequelModel(:items)
    #   validates do
    #     format_of...
    #     presence_of...
    #     acceptance_of...
    #     confirmation_of...
    #     length_of...
    #     true_for...
    #     numericality_of...
    #     format_of...
    #     validates_base...
    #     validates_each...
    #   end
    # end
    #
    # You may also perform the usual 'longhand' way to assign default model validates 
    # directly within the model class itself:
    #
    #   class MyModel < SequelModel(:items)
    #     validates_format_of...
    #     validates_presence_of...
    #     validates_acceptance_of...
    #     validates_confirmation_of...
    #     validates_length_of...
    #     validates_true_for...
    #     validates_numericality_of...
    #     validates_format_of...
    #     validates_base...
    #     validates_each...
    #   end
    #
    # Each validation allows for arguments:
    #   TODO: fill the argument options in here
    #
    # =Advanced Sequel Validations
    #
    # TODO: verify that advanced validates work as stated (aka write specs)
    # NOTE: experimental
    #
    #  To store validates for conditional usage simply specify a name with which to store them
    #    class User < Sequel::Model
    #
    #      # This set of validates becomes stored as :default and gets injected into the model.
    #      validates do
    #        # standard validates calls
    #      end
    #      
    #      validates(:registration) do
    #        # user registration specific validates 
    #      end
    #      
    #      validates(:promotion) do
    #        # user promotion validates
    #      end
    #
    #    end
    #
    # To use the above validates:
    #
    #   @user.valid?                # Runs the default validations only.
    #   @user.valid?(:registration) # Runs both default and registration validations
    #   @user.valid?(:promotion)    # Runs both default and promotion validations
    #
    # You may determine whether the model has validates via:
    #
    #   has_validations? # will return true / false based on existence of validations on the model.
    #
    # You may also retrieve the validations block if needed:
    #
    #   validates(:registration) # returns the registration validation block.
    #
    # validates() method parameters:
    #   a validations block - runs the validations block on the model & stores as :default
    #   a name and a validations block - stores the block under the name
    #   a name - returns a stored block of that name or nil
    #   nothing - returns true / false based on if validations exist for the model.
    #
    module Validations
      class Generator
        def initialize(model_class ,&block)
          @model_class = model_class
          instance_eval(&block)
        end

        def method_missing(method, *args)
          method = :"validates_#{method}"
          @model_class.send(method, *args)
        end
      end
    end

    include ::Validatable

    def self.validates(&block)
      Validations::Generator.new(self, &block)
    end

    # return true if there are validations stored, false otherwise
    def self.has_validations?
      validations.length > 0 ? true : false
    end
  end
end
