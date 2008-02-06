module Sequel::Plugins::Validated
  module InstanceMethods
    
    def self.included(base) #:nodoc:
      base.instance_eval do
        begin
          alias_method :save_without_validations, :save!
        rescue
          alias_method :save_without_validations, :save
        end
        alias_method :save, :save_with_validations
      end
    end
    
    # Returns instance of Errors
    def errors
      @errors ||= Errors.new
    end

    # Clears errors and runs validations returns true if record is valid
    def valid?
      errors.clear

      before_validation
      model.run_validations self

      errors.empty?
    end
    
    def before_validation() #:nodoc:
    end
    
    # Runs hooks before validations and raises Sequel::ValidationError
    # unless record is valid
    def save_with_validations
      if valid?
        save_without_validations
      else
        if validated_opts[:without_exception] != true
          raise ValidationError.with(errors)
        else
          false
        end
      end
    end

  end
end