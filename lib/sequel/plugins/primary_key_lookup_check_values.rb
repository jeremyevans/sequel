# frozen-string-literal: true

module Sequel
  module Plugins
    # The primary_key_lookup_check_values plugin typecasts given primary key
    # values before performing a lookup by primary key. If the given primary
    # key value cannot be typecasted correctly, the lookup returns nil
    # without issuing a query.  If the schema for the primary key column
    # includes minimum and maximum values, this also checks the given value
    # is not outside the range.  If the given value is outside the allowed
    # range, the lookup returns nil without issuing a query.
    #
    # This affects the following Model methods:
    #
    # * Model.[] (when called with non-Hash)
    # * Model.with_pk
    # * Model.with_pk!
    #
    # It also affects the following Model dataset methods:
    #
    # * Dataset#[] (when called with Integer)
    # * Dataset#with_pk
    # * dataset#with_pk!
    # 
    # Note that this can break working code.  The above methods accept
    # any filter condition by default, not just primary key values.  The
    # plugin will handle Symbol, Sequel::SQL::Expression, and
    # Sequel::LiteralString objects, but code such as the following will break:
    #
    #   # Return first Album where primary key is one of the given values
    #   Album.dataset.with_pk([1, 2, 3])
    #
    # Usage:
    #
    #   # Make all model subclasses support checking primary key values before
    #   # lookup # (called before loading subclasses)
    #   Sequel::Model.plugin :primary_key_lookup_check_values
    #
    #   # Make the Album class support checking primary key values before lookup
    #   Album.plugin :primary_key_lookup_check_values
    module PrimaryKeyLookupCheckValues
      def self.configure(model)
        model.instance_exec do
          setup_primary_key_lookup_check_values if @dataset
        end
      end

      module ClassMethods
        Plugins.after_set_dataset(self, :setup_primary_key_lookup_check_values)

        Plugins.inherited_instance_variables(self,
          :@primary_key_type=>nil,
          :@primary_key_value_range=>nil)

        private

        # Check the given primary key value.  Typecast it to the appropriate
        # database type if the database type is known.  If it cannot be
        # typecasted, or the typecasted value is outside the range of column
        # values, return nil.
        def _check_pk_lookup_value(pk)
          return if nil == pk
          case pk
          when SQL::Expression, LiteralString, Symbol
            return pk
          end
          return pk unless pk_type = @primary_key_type

          if pk_type.is_a?(Array)
            return unless pk.is_a?(Array)
            return unless pk.size == pk_type.size
            return if pk.any?(&:nil?)

            pk_value_range = @primary_key_value_range
            i = 0
            pk.map do |v|
              if type = pk_type[i]
                v = _typecast_pk_lookup_value(v, type)
                return if nil == v
                if pk_value_range
                  min, max = pk_value_range[i]
                  return if min && v < min
                  return if max && v > max
                end
              end
              i += 1
              v
            end
          elsif pk.is_a?(Array)
            return
          elsif nil != (pk = _typecast_pk_lookup_value(pk, pk_type))
            min, max = @primary_key_value_range
            return if min && pk < min
            return if max && pk > max
            pk
          end
        end

        # Typecast the value to the appropriate type,
        # returning nil if it cannot be typecasted.
        def _typecast_pk_lookup_value(value, type)
          db.typecast_value(type, value)
        rescue InvalidValue
          nil
        end

        # Skip the primary key lookup if the typecasted and checked
        # primary key value is nil.
        def primary_key_lookup(pk)
          unless nil == (pk = _check_pk_lookup_value(pk))
            super
          end
        end

        # Setup the primary key type and value range used for checking
        # primary key values during lookup.
        def setup_primary_key_lookup_check_values
          if primary_key.is_a?(Array)
            types = []
            value_ranges = []
            primary_key.each do |pk|
              type, min, max = _type_min_max_values_for_column(pk)
              types << type
              value_ranges << ([min, max].freeze if min || max)
            end
            @primary_key_type = (types.freeze if types.any?)
            @primary_key_value_range = (value_ranges.freeze if @primary_key_type && value_ranges.any?)
          else
            @primary_key_type, min, max = _type_min_max_values_for_column(primary_key)
            @primary_key_value_range = ([min, max].freeze if @primary_key_type && (min || max))
          end
        end

        # Return the type, min_value, and max_value schema entries
        # for the column, if they exist.
        def _type_min_max_values_for_column(column)
          if schema = db_schema[column]
            schema.values_at(:type, :min_value, :max_value)
          end
        end
      end

      module DatasetMethods
        # Skip the primary key lookup if the typecasted and checked
        # primary key value is nil.
        def with_pk(pk)
          unless nil == (pk = model.send(:_check_pk_lookup_value, pk))
            super
          end
        end
      end
    end
  end
end
