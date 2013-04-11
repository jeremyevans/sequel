module Sequel
  module Plugins
    # The auto_validations plugin automatically sets up three types of validations
    # for your model columns:
    #
    # 1. presence validations on NOT NULL columns
    # 2. not_string validations on non-String columns
    # 3. unique validations on columns or sets of columns with unique indexes
    #
    # To determine the columns to use for the presence and not string validations,
    # the plugin looks at the database schema for the model's table.  To determine
    # the unique validations, Sequel looks at the indexes on the table.  In order
    # for this plugin to be fully functional, the underlying database adapter needs
    # to support both schema and index parsing.
    #
    # This plugin uses the validation_helpers plugin underneath to implement the
    # validations.  It does not allow for any per-column validation message
    # customization, but you can alter the messages on a per-model basis
    # (see the validation_helpers documentation).
    #
    # You can skip certain types of validations from being automatically added via:
    #
    #   Model.skip_auto_validations(:presence)
    #
    # If you want to skip all auto validations (only useful if loading the plugin
    # in a superclass):
    #
    #   Model.skip_auto_validations(:all)
    #
    # Usage:
    #
    #   # Make all model subclass use auto validations (called before loading subclasses)
    #   Sequel::Model.plugin :auto_validations
    #
    #   # Make the Album class use auto validations
    #   Album.plugin :auto_validations
    module AutoValidations
      # Load the validation_helpers plugin and setup data structures.
      def self.apply(model)
        model.instance_eval do
          plugin :validation_helpers
          @auto_validate_presence_columns = []
          @auto_validate_not_string_columns = []
          @auto_validate_unique_columns = []
        end
      end

      # Setup auto validations for the model if it has a dataset.
      def self.configure(model)
        model.instance_eval do
          setup_auto_validations if @dataset
        end
      end

      module ClassMethods
        # The columns with automatic presence validations
        attr_reader :auto_validate_presence_columns

        # The columns with automatic not_string validations
        attr_reader :auto_validate_not_string_columns

        # The columns or sets of columns with automatic unique validations
        attr_reader :auto_validate_unique_columns

        Plugins.inherited_instance_variables(self, :@auto_validate_presence_columns=>:dup, :@auto_validate_not_string_columns=>:dup, :@auto_validate_unique_columns=>:dup)
        Plugins.after_set_dataset(self, :setup_auto_validations)

        # Skip automatic validations for the given validation type (:presence, :not_string, :unique).
        # If :all is given as the type, skip all auto validations.
        def skip_auto_validations(type)
          if type == :all
            [:presence, :not_string, :unique].each{|v| skip_auto_validations(v)}
          else
            send("auto_validate_#{type}_columns").clear
          end
        end

        private

        STRING_COLUMNS = [:string, nil, :blob].freeze

        # Parse the database schema and indexes and record the columns to automatically validate.
        def setup_auto_validations
          @auto_validate_presence_columns = db_schema.select{|col, sch| sch[:allow_null] == false && sch[:ruby_default].nil?}.map{|col, sch| col} - Array(primary_key)
          @auto_validate_not_string_columns = db_schema.reject{|col, sch| STRING_COLUMNS.include?(sch[:type])}.map{|col, sch| col} - Array(primary_key)

          @auto_validate_unique_columns = if db.respond_to?(:indexes) 
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
          validates_presence(model.auto_validate_presence_columns)
          validates_not_string(model.auto_validate_not_string_columns, :allow_nil=>true)
          model.auto_validate_unique_columns.each{|cols| validates_unique(cols)}
        end
      end
    end
  end
end
