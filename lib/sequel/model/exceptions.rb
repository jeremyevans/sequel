module Sequel
  # This exception will be raised when raise_on_save_failure is set and validation fails
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

  # This exception will be raised when raise_on_save_failure is set and a before hook returns false
  class BeforeHookFailed < Error; end
end
