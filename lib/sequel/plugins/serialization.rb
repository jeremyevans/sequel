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
    # You can use either marshal, yaml, or json as the serialization format.
    # If you use yaml or json, you should require them by yourself.
    #
    # Because of how this plugin works, it must be used inside each model class
    # that needs serialization, after any set_dataset method calls in that class.
    # Otherwise, it is possible that the default column accessors will take
    # precedence.
    #
    # == Example
    #
    #   require 'sequel'
    #   require 'json'
    #   class User < Sequel::Model
    #     plugin :serialization, :json, :permissions
    #     # or
    #     plugin :serialization
    #     serialize_attributes :marshal, :permissions, :attributes
    #   end
    #   user = User.create
    #   user.permissions = { :global => 'read-only' }
    #   user.save
    module Serialization
      # Set up the column readers to do deserialization and the column writers
      # to save the value in deserialized_values.
      def self.apply(model, *args)
        model.instance_eval{@serialization_map = {}}
      end
      
      def self.configure(model, format=nil, *columns)
        model.serialize_attributes(format, *columns) unless columns.empty?
      end

      module ClassMethods
        # A map of the serialized columns for this model.  Keys are column
        # symbols, values are serialization formats (:marshal, :yaml, or :json).
        attr_reader :serialization_map

        # Module to store the serialized column accessor methods, so they can
        # call be overridden and call super to get the serialization behavior
        attr_accessor :serialization_module

        # Copy the serialization format and columns to serialize into the subclass.
        def inherited(subclass)
          super
          sm = serialization_map.dup
          subclass.instance_eval{@serialization_map = sm}
        end
        
        # The first value in the serialization map.  This is only for
        # backwards compatibility, use serialization_map in new code.
        def serialization_format
          serialization_map.values.first
        end
        
        # Create instance level reader that deserializes column values on request,
        # and instance level writer that stores new deserialized value in deserialized
        # columns
        def serialize_attributes(format, *columns)
          raise(Error, "Unsupported serialization format (#{format}), should be :marshal, :yaml, or :json") unless [:marshal, :yaml, :json].include?(format)
          raise(Error, "No columns given.  The serialization plugin requires you specify which columns to serialize") if columns.empty?
          define_serialized_attribute_accessor(format, *columns)
        end
        
        # The columns that will be serialized.  This is only for
        # backwards compatibility, use serialization_map in new code.
        def serialized_columns
          serialization_map.keys
        end

        private

        # Add serializated attribute acessor methods to the serialization_module
        def define_serialized_attribute_accessor(format, *columns)
          m = self
          include(self.serialization_module ||= Module.new) unless serialization_module
          serialization_module.class_eval do
            columns.each do |column|
              m.serialization_map[column] = format
              define_method(column) do 
                if deserialized_values.has_key?(column)
                  deserialized_values[column]
                else
                  deserialized_values[column] = deserialize_value(column, super())
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
          deserialized_values.each{|k,v| @values[k] = serialize_value(k, v)}
          super
        end
        
        # Empty the deserialized values when refreshing.
        def refresh
          @deserialized_values = {}
          super
        end

        private

        # Deserialize the column from either marshal or yaml format
        def deserialize_value(column, v)
          return v if v.nil?
          case model.serialization_map[column] 
          when :marshal
            Marshal.load(v.unpack('m')[0]) rescue Marshal.load(v)
          when :yaml
            YAML.load v if v
          when :json
            JSON.parse v if v
          else
            raise Error, "Bad serialization format (#{model.serialization_map[column].inspect}) for column #{column.inspect}"
          end
        end

        # Serialize the column to either marshal or yaml format
        def serialize_value(column, v)
          return v if v.nil?
          case model.serialization_map[column] 
          when :marshal
            [Marshal.dump(v)].pack('m')
          when :yaml
            v.to_yaml
          when :json
            v.to_json
          else
            raise Error, "Bad serialization format (#{model.serialization_map[column].inspect}) for column #{column.inspect}"
          end
        end
      end
    end
  end
end
