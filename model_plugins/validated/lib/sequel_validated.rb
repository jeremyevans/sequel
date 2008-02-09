require File.dirname(__FILE__) + '/validated'
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
    #     is :validated
    #   end
    #
    # <b>To turn off before_validate and after_validate hooks:</b>
    #
    #   class User < Sequel::Model
    #     is :validated, :without => :hooks
    #   end
    #
    # <b>To turn off raised Exceptions if validation before save fails:</b>
    #
    #   class User < Sequel::Model
    #     # save on invalid users will return false
    #     is :validated, :without => :exceptions
    #   end
    #
    # <b>To combine those:</b>
    #
    #   class User < Sequel::Model
    #     is :validated, :without => [:exceptions, :hooks]
    #   end
    #
    class Validated < Validated::Validator
      
      # Applies plugin to a Sequel::Model.
      def self.apply(receiver, *args)
        receiver.extend ::Validated
        receiver.def_validator self, :create, :update
        receiver.extend ClassMethods
        receiver.instance_eval { alias_method :save, :save! }
        receiver.def_validated_before :save, *args
        
        without = args.extract_options![:without]
        unless [*without].include? :hooks
          receiver.instance_eval do
            [:before_validate, :after_validate].each do |m|
              define_method(m) {}
              def_hook_method(m)
            end
            
            alias_method :validate_without_hooks, :validate
            
            define_method :validate do
              before_validate
              validate_without_hooks
              after_validate
            end
          end
        end
      end
      
      # Returns state for given instance.
      def get_state(instance)
        if instance.new? then @states[:create] else @states[:update] end
      end
      
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
