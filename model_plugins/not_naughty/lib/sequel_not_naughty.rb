require File.dirname(__FILE__) + '/not_naughty'
require 'sequel'

module Sequel #:nodoc:
  module Plugins #:nodoc:
    # == Adapter for Sequel ...
    #
    # ... is a Sequel::Plugin.
    #
    # ---
    #
    # <b>To make it overall available:</b>
    #
    #   class Sequel::Model
    #     is :not_naughty
    #   end
    #
    # <b>To turn off before_validate and after_validate hooks:</b>
    #
    #   class User < Sequel::Model
    #     is :not_naughty, :without => :hooks
    #   end
    #
    # <b>To turn off raised Exceptions if validation before save fails:</b>
    #
    #   class User < Sequel::Model
    #     # save on invalid users will return false
    #     is :not_naughty, :without => :exceptions
    #   end
    #
    # <b>To combine those:</b>
    #
    #   class User < Sequel::Model
    #     is :not_naughty, :without => [:exceptions, :hooks]
    #   end
    #
    class NotNaughty < NotNaughty::Validator
      
      # Applies plugin to a Sequel::Model.
      def self.apply(receiver, *args)
        receiver.extend ::NotNaughty
        receiver.validator self, :create, :update
        
        ::NotNaughty::Validation.load(
          :acceptance, :confirmation, :format,
          :length, :numericality, :presence
        )
        
        receiver.extend ClassMethods
        receiver.instance_eval { alias_method :save, :save! }
        receiver.validated_before :save, *args
        
        without = args.extract_options![:without]
        receiver.send! :include, Hooks unless [*without].include? :hooks
      end
      
      # Returns state for given instance.
      def get_state(instance)
        if instance.new? then @states[:create] else @states[:update] end
      end
      
      # Adds validation hooks to Sequel::Model.
      module Hooks
        def self.included(base)
          base.instance_eval do
            def_hook_method :before_validate
            def_hook_method :after_validate
            alias_method :validate_without_hooks, :validate
            alias_method :validate, :validate_with_hooks
          end
        end
        
        def before_validate() end
        def after_validate() end
        
        def validate_with_hooks
          before_validate
          validate_without_hooks
          after_validate
        end
      end
      
      # Ensures Sequel::Model API compatibility.
      module ClassMethods
        
        # Returns the validations hash for the class.
        def validations
          validator.states.
          inject({}) do |validations, state_with_name|
            validations.merge(state_with_name[1].validations) {|k,o,n| o|n}
          end
        end
        # Returns true if validations are defined.
        def has_validations?()
          validator.has_validations?
        end
        # Validates the given instance.
        def validate(instance)
          validator.invoke instance
        end
        
      end
      
    end
  end
end
