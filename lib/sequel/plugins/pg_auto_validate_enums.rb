# frozen-string-literal: true

module Sequel
  module Plugins
    # The pg_auto_validate_enums plugin implements automatic validations for
    # enum columns, ensuring that enum columns have a valid value.  With this
    # plugin, trying to save with an invalid enum value results in
    # Sequel::ValidationFailed before saving, instead of Sequel::DatabaseError
    # (wrapping PG::InvalidTextRepresentation or similar exception) during saving.
    # 
    #   class Person < Sequel::Model
    #     # assume state enum column with allowed values active and inactive
    #     plugin :pg_auto_validate_enums
    #   end
    #   p = Person.new(state: "active").valid? # => true
    #   p = Person.new(state: "inactive").valid? # => true
    #   p = Person.new(state: "other").valid? # => false
    #
    # While you can load this into individual model classes, typical use would
    # be to load it into Sequel::Model or the appropriate model base class,
    # and have all models that inherit from that class automatically pick it up.
    #
    # This plugin depends on the validation_helpers plugin.
    module PgAutoValidateEnums
      # Load the validation_helpers plugin.
      def self.apply(model, opts=OPTS)
        model.plugin(:validation_helpers)
      end

      # Load the pg_enum extension into the database, and reload the schema
      # if it is already loaded. The opts given are used for the validates_includes
      # validations (with allow_nil: true and from: :values enabled by default,
      # to avoid issues with nullable enum columns and cases where the column
      # method has been overridden.
      def self.configure(model, opts=OPTS)
        model.instance_exec do
          db.extension(:pg_enum) unless @db.instance_variable_get(:@enum_labels)
          if @db_schema
            get_db_schema(true)
            _get_pg_pg_auto_validate_enums_metadata
          end
          @pg_auto_validate_enums_opts = {allow_nil: true, from: :values}.merge!(opts).freeze
        end
      end

      module ClassMethods
        # Hash with enum column symbol values and arrays of valid string values.
        attr_reader :pg_auto_validate_enums_metadata

        # Options to pass to the validates_includes calls used by the plugin.
        attr_reader :pg_auto_validate_enums_opts

        Plugins.after_set_dataset(self, :_get_pg_pg_auto_validate_enums_metadata)

        Plugins.inherited_instance_variables(self,
          :@pg_auto_validate_enums_metadata=>nil,
          :@pg_auto_validate_enums_opts=>nil)

        private

        # Parse the column schema to find columns with :enum_values entries,
        # which will be used to setup validations.
        def _get_pg_pg_auto_validate_enums_metadata
          metadata = {}
          @db_schema.each do |key, sch|
            if enum_values = sch[:enum_values]
              metadata[key] = enum_values
            end
          end
          @pg_auto_validate_enums_metadata = metadata.freeze
        end
      end

      module InstanceMethods
        # Validate that all of the model's enum columns have valid values.
        def validate
          super

          klass = self.class
          opts = klass.pg_auto_validate_enums_opts
          klass.pg_auto_validate_enums_metadata.each do |column, values|
            validates_includes(values, column, opts)
          end
        end
      end
    end
  end
end
