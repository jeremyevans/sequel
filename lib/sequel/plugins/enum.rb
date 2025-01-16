# frozen-string-literal: true

module Sequel
  module Plugins
    # The enum plugin allows for easily adding methods to modify the value of
    # a column.  It allows treating the column itself as an enum, returning a
    # symbol for the related enum value.  It also allows for setting up dataset
    # methods to easily find records having or not having each enum value.
    #
    # After loading the plugin, you can call the +enum+ method to define the
    # methods.  The +enum+ method accepts a symbol for the underlying
    # database column, and a hash with symbol keys for the enum values.
    # For example, the following call:
    #
    #   Album.enum :status_id, good: 1, bad: 2
    #
    # Will define the following instance methods:
    #
    # Album#good! :: Change +status_id+ to +1+ (does not save the receiver)
    # Album#bad! :: Change +status_id+ to +2+ (does not save the receiver)
    # Album#good? :: Return whether +status_id+ is +1+
    # Album#bad? :: Return whether +status_id+ is +2+
    #
    # It will override the following instance methods:
    #
    # Album#status_id :: Return +:good+/+:bad+ instead of +1+/+2+ (other values returned as-is)
    # Album#status_id= :: Allow calling with +:good+/+:bad+ to set +status_id+ to +1+/+2+ (other values,
    #                     such as <tt>'good'</tt>/<tt>'bad'</tt> set as-is)
    #
    # If will define the following dataset methods:
    #
    # Album.dataset.good :: Return a dataset filtered to rows where +status_id+ is +1+
    # Album.dataset.not_good :: Return a dataset filtered to rows where +status_id+ is not +1+
    # Album.dataset.bad:: Return a dataset filtered to rows where +status_id+ is +2+
    # Album.dataset.not_bad:: Return a dataset filtered to rows where +status_id+ is not +2+
    #
    # When calling +enum+, you can also provide the following options:
    #
    # :prefix :: Use a prefix for methods defined for each enum value. If +true+ is provided at the value, use the column name as the prefix.
    #            For example, with <tt>prefix: 'status'</tt>, the instance methods defined above would be +status_good?+, +status_bad?+,
    #            +status_good!+, and +status_bad!+, and the dataset methods defined would be +status_good+, +status_not_good+, +status_bad+,
    #            and +status_not_bad+.
    # :suffix :: Use a suffix for methods defined for each enum value. If +true+ is provided at the value, use the column name as the suffix.
    #            For example, with <tt>suffix: 'status'</tt>, the instance methods defined above would be +good_status?+, +bad_status?+,
    #            +good_status!+, and +bad_status!+, and the dataset methods defined would be +good_status+, +not_good_status+, +bad_status+,
    #            and +not_bad_status+.
    # :override_accessors :: Set to +false+ to not override the column accessor methods.
    # :dataset_methods :: Set to +false+ to not define dataset methods.
    #
    # Note that this does not use a true enum column in the database.  If you are
    # looking for enum support in the database, and your are using PostgreSQL,
    # Sequel supports that via the pg_enum Database extension.
    #
    # Usage:
    #
    #   # Make all model subclasses handle enums
    #   Sequel::Model.plugin :enum
    #
    #   # Make the Album class handle enums
    #   Album.plugin :enum
    module Enum
      module ClassMethods
        # Define instance and dataset methods in this class to treat column
        # as a enum.  See Enum documentation for usage.
        def enum(column, values, opts=OPTS)
          raise Sequel::Error, "enum column must be a symbol" unless column.is_a?(Symbol)
          raise Sequel::Error, "enum values must be provided as a hash with symbol keys" unless values.is_a?(Hash) && values.all?{|k,| k.is_a?(Symbol)}

          if prefix = opts[:prefix]
            prefix = column if prefix == true
            prefix = "#{prefix}_"
          end

          if suffix = opts[:suffix]
            suffix = column if suffix == true
            suffix = "_#{suffix}"
          end
          
          values = Hash[values].freeze
          inverted = values.invert.freeze

          unless @enum_methods
            @enum_methods = Sequel.set_temp_name(Module.new){"#{name}::@enum_methods"}
            include @enum_methods
          end

          @enum_methods.module_eval do
            unless opts[:override_accessors] == false
              define_method(column) do
                v = super()
                inverted.fetch(v, v)
              end

              define_method(:"#{column}=") do |v|
                super(values.fetch(v, v))
              end
            end

            values.each do |key, value|
              define_method(:"#{prefix}#{key}#{suffix}!") do
                self[column] = value
                nil
              end

              define_method(:"#{prefix}#{key}#{suffix}?") do
                self[column] == value
              end
            end
          end

          unless opts[:dataset_methods] == false
            dataset_module do
              values.each do |key, value|
                cond = Sequel[column=>value]
                where :"#{prefix}#{key}#{suffix}", cond
                where :"#{prefix}not_#{key}#{suffix}", ~cond
              end
            end
          end
        end
      end
    end
  end
end
