# frozen-string-literal: true

module Sequel
  module Plugins
    # The pg_eager_any_typed_array plugin automatically converts
    # the predicate expressions used for eager loading from:
    #
    #   table.column IN (value_list)
    #
    # to:
    #
    #   table.column = ANY(array_expr::type[])
    #
    # This makes it easier to use the pg_auto_parameterize_in_array
    # extension with the :treat_string_list_as_text_array option,
    # when using foreign keys with non-text database types that are represented
    # by Ruby strings, such as enum and uuid types.
    #
    # To avoid this behavior for particular associations, set the
    # :eager_loading_predicate_transform association option to nil/false.
    #
    # This plugin loads the pg_array extension into the model's Database.
    module PgEagerAnyTypedArray
      # Add the pg_array extension to the database
      def self.apply(model)
        model.db.extension(:pg_array)
      end

      module ClassMethods
        TRANSFORM = proc do |values, ref|
          type = ref.send(:cached_fetch, :_pg_eager_any_typed_array_type) do
            key = ref.predicate_key
            next if key.is_a?(Array)

            while key.is_a?(SQL::QualifiedIdentifier)
              key = key.column
            end

            if (sch = ref.associated_class.db_schema[key])
              sch[:db_type]
            end
          end

          if type
            Sequel.function(:ANY, Sequel.pg_array(values, type))
          else
            values
          end
        end

        # If the association does not use a composite predicate key,
        # and does not already have the :eager_loading_predicate_transform
        # option set, set the option so that eager loading
        def associate(type, name, opts = OPTS, &block)
          res = super

          unless res.has_key?(:eager_loading_predicate_transform)
            res[:eager_loading_predicate_transform] = TRANSFORM
          end

          res
        end
      end
    end
  end
end
