module Sequel
  module Plugins
    # The auto_validations plugin automatically sets up the following types of validations
    # for your model columns:
    #
    # 1. type validations for all columns
    # 2. not_null validations on NOT NULL columns (optionally, presence validations)
    # 3. unique validations on columns or sets of columns with unique indexes
    # 4. max length validations on string columns
    #
    # To determine the columns to use for the type/not_null/max_length validations,
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
    # This is useful if you want to enforce that NOT NULL string columns do not
    # allow empty values.
    #
    # Usage:
    #
    #   # Make all model subclass use auto validations (called before loading subclasses)
    #   Sequel::Model.plugin :auto_validations
    #
    #   # Make the Album class use auto validations
    #   Album.plugin :auto_validations
    module AutoValidations
      def self.apply(model, opts=OPTS)
        model.instance_eval do
          plugin :validation_helpers
          @auto_validate_presence = false
          @auto_validate_not_null_columns = []
          @auto_validate_explicit_not_null_columns = []
          @auto_validate_max_length_columns = []
          @auto_validate_unique_columns = []
          @auto_validate_types = true
        end
      end

      # Setup auto validations for the model if it has a dataset.
      def self.configure(model, opts=OPTS)
        model.instance_eval do
          setup_auto_validations if @dataset
          if opts[:not_null] == :presence
            @auto_validate_presence = true
          end
        end
      end

      module ClassMethods
        # The columns with automatic not_null validations
        attr_reader :auto_validate_not_null_columns

        # The columns with automatic not_null validations for columns present in the values.
        attr_reader :auto_validate_explicit_not_null_columns

        # The columns or sets of columns with automatic max_length validations, as an array of
        # pairs, with the first entry being the column name and second entry being the maximum length.
        attr_reader :auto_validate_max_length_columns

        # The columns or sets of columns with automatic unique validations
        attr_reader :auto_validate_unique_columns

        Plugins.inherited_instance_variables(self, :@auto_validate_presence=>nil, :@auto_validate_types=>nil, :@auto_validate_not_null_columns=>:dup, :@auto_validate_explicit_not_null_columns=>:dup, :@auto_validate_max_length_columns=>:dup, :@auto_validate_unique_columns=>:dup)
        Plugins.after_set_dataset(self, :setup_auto_validations)

        # Whether to use a presence validation for not null columns
        def auto_validate_presence?
          @auto_validate_presence
        end

        # Whether to automatically validate schema types for all columns
        def auto_validate_types?
          @auto_validate_types
        end

        # Skip automatic validations for the given validation type (:not_null, :types, :unique).
        # If :all is given as the type, skip all auto validations.
        def skip_auto_validations(type)
          if type == :all
            [:not_null, :types, :unique, :max_length].each{|v| skip_auto_validations(v)}
          elsif type == :types
            @auto_validate_types = false
          else
            send("auto_validate_#{type}_columns").clear
          end
        end

        private

        # Parse the database schema and indexes and record the columns to automatically validate.
        def setup_auto_validations
          not_null_cols, explicit_not_null_cols = db_schema.select{|col, sch| sch[:allow_null] == false}.partition{|col, sch| sch[:ruby_default].nil?}.map{|cs| cs.map{|col, sch| col}}
          @auto_validate_not_null_columns = not_null_cols - Array(primary_key)
          explicit_not_null_cols += Array(primary_key)
          @auto_validate_explicit_not_null_columns = explicit_not_null_cols.uniq
          @auto_validate_max_length_columns = db_schema.select{|col, sch| sch[:type] == :string && sch[:max_length].is_a?(Integer)}.map{|col, sch| [col, sch[:max_length]]}
          table = dataset.first_source_table
          @auto_validate_unique_columns = if db.supports_index_parsing? && [Symbol, SQL::QualifiedIdentifier, SQL::Identifier, String].any?{|c| table.is_a?(c)}
            db.indexes(table).select{|name, idx| idx[:unique] == true}.map{|name, idx| idx[:columns]}
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
          unless (not_null_columns = model.auto_validate_explicit_not_null_columns).empty?
            if model.auto_validate_presence?
              validates_presence(not_null_columns, :allow_missing=>true)
            else
              validates_not_null(not_null_columns, :allow_missing=>true)
            end
          end
          unless (max_length_columns = model.auto_validate_max_length_columns).empty?
            max_length_columns.each do |col, len|
              validates_max_length(len, col, :allow_nil=>true)
            end
          end

          validates_schema_types if model.auto_validate_types?

          unique_opts = {}
          if model.respond_to?(:sti_dataset)
            unique_opts[:dataset] = model.sti_dataset
          end
          model.auto_validate_unique_columns.each{|cols| validates_unique(cols, unique_opts)}
        end
      end
    end
  end
end
