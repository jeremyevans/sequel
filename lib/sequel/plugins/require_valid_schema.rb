# frozen-string-literal: true

module Sequel
  module Plugins
    # The require_valid_schema plugin makes Sequel raise or warn if attempting
    # to set the dataset of a model class to a simple table, where the database
    # supports schema parsing, but schema parsing does not work for the model's
    # table.
    #
    # The plugin's default behavior requires that all models that select from a
    # single identifier have a valid table schema, if the database supports
    # schema parsing. If the schema cannot be determined for such
    # a model, an error is raised:
    #   
    #   Sequel::Model.plugin :require_valid_schema
    #
    # If you load the plugin with an argument of :warn, Sequel will warn instead
    # of raising for such tables:
    #
    #   Sequel::Model.plugin :require_valid_schema, :warn
    #
    # This can catch bugs where you expect models to have valid schema, but
    # they do not. This setting only affects future attempts to set datasets
    # in the current class and subclasses created in the future.
    #
    # If you load the plugin with an argument of false, it will not require valid schema.
    # This can be used in subclasses where you do not want to require valid schema,
    # but the plugin must be loaded before a dataset with invalid schema is set:
    #
    #   Sequel::Model.plugin :require_valid_schema
    #   InvalidSchemaAllowed = Class.new(Sequel::Model)
    #   InvalidSchemaAllowed.plugin :require_valid_schema, false
    #   class MyModel < InvalidSchemaAllowed
    #   end
    module RequireValidSchema
      # Modify the current model's dataset selection, if the model
      # has a dataset.
      def self.configure(model, setting=true)
        model.instance_variable_set(:@require_valid_schema, setting)
      end

      module ClassMethods
        Plugins.inherited_instance_variables(self, :@require_valid_schema=>nil)

        private

        # If the schema cannot be determined, the model uses a simple table,
        # require_valid_schema is set, and the database supports schema parsing, raise or
        # warn based on the require_valid_schema setting.
        def get_db_schema_array(reload)
          schema_array = super

          if !schema_array && simple_table && @require_valid_schema
            message = "Not able to parse schema for model: #{inspect}, table: #{simple_table}"
            if @require_valid_schema == :warn
              warn message
            else
              raise Error, message
            end
          end

          schema_array
        end
      end
    end
  end
end
