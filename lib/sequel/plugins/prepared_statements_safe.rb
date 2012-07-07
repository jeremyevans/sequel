module Sequel
  module Plugins
    # The prepared_statements_safe plugin modifies the model to reduce the number of
    # prepared statements that can be created, by setting as many columns as possible
    # before creating, and by changing +save_changes+ to save all columns instead of
    # just the changed ones.
    #
    # This plugin depends on the +prepared_statements+ plugin.
    #
    # Usage:
    #
    #   # Make all model subclasses more safe when using prepared statements (called before loading subclasses)
    #   Sequel::Model.plugin :prepared_statements_safe
    #
    #   # Make the Album class more safe when using prepared statements
    #   Album.plugin :prepared_statements_safe
    module PreparedStatementsSafe
      # Depend on the prepared_statements plugin
      def self.apply(model)
        model.plugin(:prepared_statements)
      end

      # Set the column defaults to use when creating on the model.
      def self.configure(model)
        model.send(:set_prepared_statements_column_defaults)
      end

      module ClassMethods
        # A hash with column symbol keys and default values.  Instance's
        # values are merged into this hash before creating to reduce the
        # number of free columns (columns that may or may not be present
        # in the INSERT statement), as the number of prepared statements
        # that can be created is 2^N (where N is the number of free columns).
        attr_reader :prepared_statements_column_defaults

        def inherited(subclass)
          super
          subclass.instance_variable_set(:@prepared_statements_column_defaults, @prepared_statements_column_defaults) if @prepared_statements_column_defaults && !subclass.prepared_statements_column_defaults
        end

        # Set the column defaults to use when creating on the subclass.
        def set_dataset(*)
          x = super
          set_prepared_statements_column_defaults
          x
        end

        private

        # Set the column defaults based on the database schema.  All columns
        # are set to a default value unless they are a primary key column or
        # they don't have a parseable default.
        def set_prepared_statements_column_defaults
          if db_schema
            h = {}
            db_schema.each do |k, v|
              h[k] = v[:ruby_default] if (v[:ruby_default] || !v[:default]) && !v[:primary_key]
            end
            @prepared_statements_column_defaults = h
          end
        end
      end

      module InstanceMethods
        # Merge the current values into the default values to reduce the number
        # of free columns.
        def before_create
          if v = model.prepared_statements_column_defaults
            set_values(v.merge(values))
          end
          super
        end

        # Always do a full save of all columns to reduce the number of prepared
        # statements that can be used.
        def save_changes(opts={})
          save(opts) || false if modified?
        end
      end
    end
  end
end
