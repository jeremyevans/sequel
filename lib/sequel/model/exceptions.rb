module Sequel
  # Exception class raised when +raise_on_save_failure+ is set and a before hook returns false
  class BeforeHookFailed < Error; end
  
  # Exception class raised when +require_modification+ is set and an UPDATE or DELETE statement to modify the dataset doesn't
  # modify a single row.
  class NoExistingObject < Error; end
  
  # Raised when an undefined association is used when eager loading.
  class UndefinedAssociation < Error; end
  
  # Exception class raised when +raise_on_save_failure+ is set and validation fails
  class ValidationFailed < Error
    def initialize(errors)
      if errors.respond_to?(:full_messages)
        @errors = errors
        super(errors.full_messages.join(', '))
      else
        super
      end
    end
    attr_reader :errors
  end
end
