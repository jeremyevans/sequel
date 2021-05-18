# frozen-string-literal: true

module Sequel
  module Plugins
    # The auto_validations_constraint_validations_presence_message plugin provides
    # integration for the auto_validations and constraint_validations plugins in
    # the following situation:
    #
    # * A column has a NOT NULL constraint in the database
    # * A constraint validation for presence exists on the column, with a :message
    #   option to set a column-specific message, and with the :allow_nil option set
    #   to true because the CHECK constraint doesn't need to check for NULL values
    #   as the column itself is NOT NULL
    # 
    # In this case, by default the validation error message on the column will
    # use the more specific constraint validation error message if the column
    # has a non-NULL empty value, but will use the default auto_validations
    # message if the column has a NULL value.  With this plugin, the column-specific
    # constraint validation error message will be used in both cases.
    #
    # Usage:
    #
    #   # Make all model subclasses use this auto_validations/constraint_validations
    #   # integration (called before loading subclasses)
    #   Sequel::Model.plugin :auto_validations_constraint_validations_presence_message
    #
    #   # Make the Album class use this auto_validations/constraint_validations integration
    #   Album.plugin :auto_validations_constraint_validations_presence_message
    module AutoValidationsConstraintValidationsPresenceMessage
      def self.apply(model)
        model.plugin :auto_validations
        model.plugin :constraint_validations
      end

      def self.configure(model, opts=OPTS)
        model.send(:_adjust_auto_validations_constraint_validations_presence_message)
      end

      module ClassMethods
        Plugins.after_set_dataset(self, :_adjust_auto_validations_constraint_validations_presence_message)

        private

        def _adjust_auto_validations_constraint_validations_presence_message
          if @dataset &&
             !@auto_validate_options[:not_null][:message] &&
             !@auto_validate_options[:explicit_not_null][:message]

            @constraint_validations.each do |array|
              meth, column, opts = array

              if meth == :validates_presence &&
                 opts &&
                 opts[:message] &&
                 opts[:allow_nil] &&
                 (@auto_validate_not_null_columns.include?(column) || @auto_validate_explicit_not_null_columns.include?(column))

                @auto_validate_not_null_columns.delete(column)
                @auto_validate_explicit_not_null_columns.delete(column)
                array[2] = array[2].merge(:allow_nil=>false)
              end
            end
          end
        end
      end
    end
  end
end
