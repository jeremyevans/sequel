# frozen-string-literal: true

module Sequel
  module Plugins
    # The validation_contexts plugin adds support for a validation_context method inside a validate
    # method.  You pass in the validation context to use via the :validation_context option to
    # Sequel::Model#save, #validate!, and #valid?:
    #
    #   class Album < Sequel::Model
    #     plugin :validation_contexts
    #     def validate
    #       super
    #       errors.add(:status_id, 'not 1') if status_id != 1 && validation_context == :initial
    #       errors.add(:status_id, 'not 2') if status_id != 2 && validation_context == :approve
    #     end
    #   end
    #
    #   Album.new(status_id: 1).valid?(validation_context: :initial) # => true
    #   Album.new(status_id: 2).valid?(validation_context: :initial) # => false
    #
    #   Album.new(status_id: 1).valid?(validation_context: :approve) # => false
    #   Album.new(status_id: 2).valid?(validation_context: :approve) # => true
    #
    # There is no validation context used by default, so validation_context will be
    # +nil+ if one is not specified.  If you want to differentiate between creating new
    # objects and updating existing objects, just use +new?+.
    #
    # Once this plugin is loaded into a model, after you freeze an instance
    # of that model, you can no longer specify a validation context when
    # validating the instance.
    module ValidationContexts
      module ClassMethods
        Plugins.model_instance_variables(self, :@validation_context)
      end

      module InstanceMethods 
        # The validation context to use for the current validation.
        # Set via the :validation_context option passed to save/valid?.
        attr_reader :validation_context

        def save(opts=OPTS)
          validation_context = @validation_context
          @validation_context = opts[:validation_context] if opts[:validation_context]
          super
        ensure
          @validation_context = validation_context if @validation_context
        end

        def validate!(validation_context: nil, validate: nil, raise_on_failure: nil)
          valid_context = @validation_context
          @validation_context = validation_context unless validation_context.nil?
          super(validate: validate, raise_on_failure: raise_on_failure)
        ensure
          @validation_context = valid_context if @validation_context
        end

        # Set validation context before running validations
        def valid?(opts=OPTS)
          validation_context = @validation_context
          @validation_context = opts[:validation_context] if opts[:validation_context]
          super
        ensure
          @validation_context = validation_context if @validation_context
        end
      end
    end
  end
end
