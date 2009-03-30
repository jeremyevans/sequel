module Sequel
  # This exception will be raised when raise_on_save_failure is set and validation fails
  class ValidationFailed < Error; end

  # This exception will be raised when raise_on_save_failure is set and a before hook returns false
  class BeforeHookFailed < Error; end
end
