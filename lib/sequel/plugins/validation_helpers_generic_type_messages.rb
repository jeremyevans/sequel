# frozen-string-literal: true

require_relative 'validation_helpers'

module Sequel
  module Plugins
    # The validation_helpers_generic_type_messages plugin overrides the default
    # type validation failure messages in the validation_helpers plugin to be
    # more generic and understandable by the average user, instead of always
    # be based on the names of the allowed classes for the type.  For example:
    #
    #   # :blob type
    #   # validation_helpers default: "value is not a valid sequel::sql::blob"
    #   # with this plugin: "value is not a blob"
    #
    #   # :boolean type
    #   # validation_helpers default: "value is not a valid trueclass or falseclass"
    #   # with this plugin: "value is not true or false"
    #
    #   # :datetime type
    #   # validation_helpers default: "value is not a valid time or datetime"
    #   # with this plugin: "value is not a valid timestamp"
    #
    #   # custom/database-specific types
    #   # validation_helpers default: "value is not a valid sequel::class_name"
    #   # with this plugin: "value is not the expected type"
    #
    # It is expected that this plugin will become the default behavior of
    # validation_helpers in Sequel 6.
    #
    # To enable this the use of generic type messages for all models, load this
    # plugin into Sequel::Model.
    #
    #   Sequel::Model.plugin :validation_helpers_generic_type_messages
    module ValidationHelpersGenericTypeMessages
      OVERRIDE_PROC = ValidationHelpers::DEFAULT_OPTIONS[:type][:message]
      private_constant :OVERRIDE_PROC

      TYPE_ERROR_STRINGS = {
        String => 'is not a string'.freeze,
        Integer => 'is not an integer'.freeze,
        Date => 'is not a valid date'.freeze,
        [Time, DateTime].freeze => 'is not a valid timestamp'.freeze,
        Sequel::SQLTime => 'is not a valid time'.freeze,
        [TrueClass, FalseClass].freeze => 'is not true or false'.freeze,
        Float => 'is not a number'.freeze,
        BigDecimal => 'is not a number'.freeze,
        Sequel::SQL::Blob => 'is not a blob'.freeze,
      }
      TYPE_ERROR_STRINGS.default = "is not the expected type".freeze
      TYPE_ERROR_STRINGS.freeze
      private_constant :TYPE_ERROR_STRINGS

      def self.apply(mod)
        mod.plugin :validation_helpers
      end

      module InstanceMethods 
        private

        # Use a generic error message for type validations.
        def validates_type_error_message(m, klass)
          # SEQUEL6: Make this the default behavior in validation_helpers
          if OVERRIDE_PROC.equal?(m)
            TYPE_ERROR_STRINGS[klass]
          else
            super
          end
        end
      end
    end
  end
end
