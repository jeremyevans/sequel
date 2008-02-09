module Validated
  module Builder

    # Observer method that creates Validation builder methods.
    #
    # A Validation with the class name TestValidation will get the
    # builder-method <tt>validate_test_of</tt> that'll be added via
    # add_validation in the current <tt>validator</tt>.
    def self.update(validation)
      if basename = validation.name[/([^:]+)Validation$/, 1]
        define_method 'validates_%s_of' % basename.downcase do |*params|
          validator.add_validation(validation, *params)
        end
      end
    end
    
    # Syntactic sugar.
    #
    # <b>Example:</b>
    #   validates { presence_of :example, :if => :reading? }
    #     # => validate_presence_of :example, :if => :reading?
    #   validates(:name) { presence and length :minimum => 6 }
    #     # => validates_presence_of :name
    #          validates_length_of :name, :minimum => 6
    #   validates(:temp, :if => water?) { value :lt => 100 }
    #     # => validates_value_of :temp, :lt => 100, :if => water?
    def validates(*params, &block)
      ValidationDelegator.new(self, *params).instance_eval(&block)
    end
    
    # Allows adding validations the legacy way.
    def validates_each(*attributes, &block)
      validator.add_validation(*attributes, &block)
    end
    
    require 'delegate'
    
    class ValidationDelegator < SimpleDelegator #:nodoc:all
      def initialize(receiver, *params)
        @_sd_obj_opts   = params.extract_options!
        @_sd_obj_params = params
        
        super receiver
      end
      def method_missing(method_sym, *params) #:nodoc:
        method_sym, params = if @_sd_obj_params.any?
          opts = @_sd_obj_opts.update params.extract_options!
          [:"validates_#{method_sym}_of", @_sd_obj_params + [opts]]
        else
          opts = @_sd_obj_opts.update params.extract_options!
          [:"validates_#{method_sym}", params + [opts]]
        end
        
        if @_sd_obj.respond_to? method_sym
          @_sd_obj.send!(method_sym, *params)
          return true
        else
          raise NoMethodError,
            "unable to evaluate ´#{method_sym}(*#{params.inspect})'"
        end
      end
    end
    
  end
end
