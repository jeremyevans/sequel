module Sequel
  module Plugins
    # The auto_validations plugin automatically sets up three types of validations
    # for your model columns:
    #
    # 1. type validations for all columns
    # 2. not_null validations on NOT NULL columns (optionally, presence validations)
    # 3. unique validations on columns or sets of columns with unique indexes
    #
    # To determine the columns to use for the presence validations and the types for the type validations,
    # the plugin looks at the database schema for the model's table.  To determine
    # the unique validations, Sequel looks at the indexes on the table.  In order
    # for this plugin to be fully functional, the underlying database adapter needs
    # to support both schema and index parsing.
    #
    # This plugin uses the validation_helpers plugin underneath to implement the
    # validations.  It does not allow for any per-column validation message
    # customization, but you can alter the messages for the given type of validation
    # on a per-model basis (see the validation_helpers documentation).
    #
    # You can skip certain types of validations from being automatically added via:
    #
    #   Model.skip_auto_validations(:not_null)
    #
    # If you want to skip all auto validations (only useful if loading the plugin
    # in a superclass):
    #
    #   Model.skip_auto_validations(:all)
    #
    # By default, the plugin uses a not_null validation for NOT NULL columns, but that
    # can be changed to a presence validation using an option:
    #
    #   Model.plugin :auto_validations, :not_null=>:presence
    #
    # Usage:
    #
    #   # Make all model subclass use auto validations (called before loading subclasses)
    #   Sequel::Model.plugin :auto_validations
    #
    #   # Make the Album class use auto validations
    #   Album.plugin :auto_validations
    module AutoValidations
      def self.apply(model, opts={})
        model.instance_eval do
          plugin :validation_helpers
          @auto_validate_presence = false
          @auto_validate_not_null_columns = []
          @auto_validate_unique_columns = []
          @auto_validate_types = true
        end
      end

      # Setup auto validations for the model if it has a dataset.
      def self.configure(model, opts={})
        model.instance_eval do
          setup_auto_validations if @dataset
          if opts[:not_null] == :presence
            @auto_validate_presence = true
          end
        end
      end

      module ClassMethods
        # The columns with automatic presence validations
        attr_reader :auto_validate_not_null_columns

        # The columns or sets of columns with automatic unique validations
        attr_reader :auto_validate_unique_columns

        Plugins.inherited_instance_variables(self, :@auto_validate_presence=>nil, :@auto_validate_types=>nil, :@auto_validate_not_null_columns=>:dup, :@auto_validate_unique_columns=>:dup)
        Plugins.after_set_dataset(self, :setup_auto_validations)

        # REMOVE40
        def auto_validate_presence_columns
          Sequel::Deprecation.deprecate('Model.auto_validate_presence_columns', 'Please switch to auto_validate_not_null_columns')
          auto_validate_not_null_columns
        end

        # Whether to use a presence validation for not null columns
        def auto_validate_presence?
          @auto_validate_presence
        end

        # Whether to automatically validate schema types for all columns
        def auto_validate_types?
          @auto_validate_types
        end

        # Skip automatic validations for the given validation type (:presence, :types, :unique).
        # If :all is given as the type, skip all auto validations.
        def skip_auto_validations(type)
          if type == :all
            [:not_null, :types, :unique].each{|v| skip_auto_validations(v)}
          elsif type == :types
            @auto_validate_types = false
          else
            send("auto_validate_#{type}_columns").clear
          end
        end

        private

        # Parse the database schema and indexes and record the columns to automatically validate.
        def setup_auto_validations
          @auto_validate_not_null_columns = db_schema.select{|col, sch| sch[:allow_null] == false && sch[:ruby_default].nil?}.map{|col, sch| col} - Array(primary_key)
          @auto_validate_unique_columns = if db.supports_index_parsing?
            db.indexes(dataset.first_source_table).select{|name, idx| idx[:unique] == true}.map{|name, idx| idx[:columns]}
          else
            []
          end
        end
      end

      module InstanceMethods
        # Validate the model's auto validations columns
        def validate
          super
          unless (not_null_columns = model.auto_validate_not_null_columns).empty?
            if model.auto_validate_presence?
              validates_presence(not_null_columns)
            else
              validates_not_null(not_null_columns)
            end
          end

          validates_schema_types if model.auto_validate_types?

          model.auto_validate_unique_columns.each{|cols| validates_unique(cols)}
        end
      end
    end
  end
end
