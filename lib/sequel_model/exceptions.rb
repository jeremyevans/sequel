module Sequel
  # This exception will be raised when raise_on_save_failure is set and a validation fails
  class ValidationFailed < Error;end
end
