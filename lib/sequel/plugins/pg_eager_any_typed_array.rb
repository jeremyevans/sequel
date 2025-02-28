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
    # Most association types that ship with Sequel have their predicate
    # expressions converted by this plugin.  Here are the exceptions:
    #
    # * associations using composite predicate keys
    # * many_to_pg_array associations
    # * many_to_many/one_through_one associations using :join_table_db option
    # * many_through_many/one_through_many associations using
    #   :separate_table_per_query option
    #
    # To avoid predicate conversion for particular associations, set the
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

            # :nocov:
            # many_to_pg_array association type does not need changes, as it
            # already converts the values to a typed postgres array, it does
            # not call the code that uses :eager_loading_predicate_transform.
            #
            # No association type that ships with Sequel can reach this code
            # unless it is one of these types, but external association types
            # could potentially reach it.
            sch = case ref[:type]
            # :nocov:
            when :many_to_one, :one_to_one, :one_to_many, :pg_array_to_many
              ref.associated_class.db_schema
            when :many_to_many, :one_through_one
              # Not compatible with the :join_table_db option, but that option
              # does not call into this code.
              Hash[ref.associated_class.db.schema(ref.join_table_source)]
            when :many_through_many, :one_through_many
              # Not compatible with the :separate_query_per_table option, but
              # that option does not call into this code.
              Hash[ref.associated_class.db.schema(ref[:through][0][:table])]
            end

            if sch && (sch = sch[key])
              sch[:db_type]
            end
          end

          if type
            Sequel.function(:ANY, Sequel.pg_array(values, type))
          else
            values
          end
        end

        # Set the :eager_loading_predicate_transform option if not already set
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
