module Sequel
  module Plugins
    # Sequel's built in Serialization plugin allows you to keep serialized
    # ruby objects in the database, while giving you deserialized objects
    # when you call an accessor.
    # 
    # This plugin works by keeping the serialized value in the values, and
    # adding a @deserialized_values hash.  The reader method for serialized columns
    # will check the @deserialized_values for the value, return it if present,
    # or deserialized the entry in @values and return it.  The writer method will
    # set the @deserialized_values entry.  This plugin adds a before_save hook
    # that serializes all @deserialized_values to @values.
    #
    # You can use either marshal or yaml as the serialization format.
    # If you use yaml, you should require yaml yourself.
    #
    # Because of how this plugin works, it must be used inside each model class
    # that needs serialization, after any set_dataset method calls in that class.
    # Otherwise, it is possible that the default column accessors will take
    # precedence.
    module Serialization
      # Set up the column readers to do deserialization and the column writers
      # to save the value in deserialized_values.
      def self.apply(model, format, *columns)
        raise(Error, "Unsupported serialization format (#{format}), should be :marshal or :yaml") unless [:marshal, :yaml].include?(format)
        raise(Error, "No columns given.  The serialization plugin requires you specify which columns to serialize") if columns.empty?
        model.instance_eval do
          @serialization_format = format
          @serialized_columns = columns
          InstanceMethods.module_eval do
            columns.each do |column|
              define_method(column) do 
                if deserialized_values.has_key?(column)
                  deserialized_values[column]
                else
                  deserialized_values[column] = deserialize_value(@values[column])
                end
              end
              define_method("#{column}=") do |v| 
                changed_columns << column unless changed_columns.include?(column)
                deserialized_values[column] = v
              end
            end
          end
        end
      end

      module ClassMethods
        # The serialization format to use, should be :marshal or :yaml
        attr_reader :serialization_format

        # The columns to serialize
        attr_reader :serialized_columns

        # Copy the serialization format and columns to serialize into the subclass.
        def inherited(subclass)
          super
          sf = serialization_format
          sc = serialized_columns
          subclass.instance_eval do
            @serialization_format = sf
            @serialized_columns = sc
          end
        end
      end

      module InstanceMethods
        # Hash of deserialized values, used as a cache.
        attr_reader :deserialized_values

        # Set @deserialized_values to the empty hash
        def initialize(*args, &block)
          @deserialized_values = {}
          super
        end

        # Serialize all deserialized values
        def before_save
          super
          deserialized_values.each do |k,v|
            @values[k] = serialize_value(v)
          end
        end
        
        # Empty the deserialized values when refreshing.
        def refresh
          @deserialized_values = {}
          super
        end

        private

        # Deserialize the column from either marshal or yaml format
        def deserialize_value(v)
          return v if v.nil?
          case model.serialization_format 
          when :marshal
            Marshal.load(v.unpack('m')[0]) rescue Marshal.load(v)
          when :yaml
            YAML.load v if v
          end
        end

        # Serialize the column to either marshal or yaml format
        def serialize_value(v)
          return v if v.nil?
          case model.serialization_format 
          when :marshal
            [Marshal.dump(v)].pack('m')
          when :yaml
            v.to_yaml
          end
        end
      end
    end
  end
end
