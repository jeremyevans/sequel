# frozen-string-literal: true

module Sequel
  # Exception class raised when +raise_on_save_failure+ is set and a before hook returns false
  # or an around hook doesn't call super or yield.
  class HookFailed < Error
    # The Sequel::Model instance related to this error.
    attr_reader :model

    def initialize(message=nil, model=nil)
      @model = model
      super(message)
    end
  end

  # Alias for HookFailed, kept for backwards compatibility
  BeforeHookFailed = HookFailed
  
  # Exception class raised when +require_modification+ is set and an UPDATE or DELETE statement to modify the dataset doesn't
  # modify a single row.
  NoExistingObject = Class.new(Error)
  
  # Raised when an undefined association is used when eager loading.
  UndefinedAssociation = Class.new(Error)

  # Raised when a mass assignment method is called in strict mode with either a restricted column
  # or a column without a setter method.
  MassAssignmentRestriction = Class.new(Error)
  
  # Exception class raised when +raise_on_save_failure+ is set and validation fails
  class ValidationFailed < Error
    # The Sequel::Model object related to this exception.
    attr_reader :model

    # The Sequel::Model::Errors object related to this exception.
    attr_reader :errors

    def initialize(errors=nil)
      if errors.is_a?(Sequel::Model)
        @model = errors
        errors = @model.errors
      end

      if errors.respond_to?(:full_messages)
        @errors = errors
        super(errors.full_messages.join(', '))
      else
        super
      end
    end
  end

  # Call name on each class to set the name for the class, so it gets cached.
  constants.map{|c| const_get(c)}.each do |c|
    Class === c && c.name
  end
end
