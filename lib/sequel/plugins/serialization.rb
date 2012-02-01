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
    # You can specify the serialization format as a pair of serializer/deserializer
    # callable objects.  You can also specify the serialization format as a single
    # symbol, if such a symbol has a registered serializer/deserializer pair in the
    # plugin.  By default, the plugin registers the :marshal, :yaml, and :json
    # serialization formats.  To register your own serialization formats, use
    # Sequel::Plugins::Serialization.register_format.
    # If you use yaml or json format, you need to require the libraries, Sequel
    # does not do the requiring for you.
    #
    # You can specify the columns to serialize when loading the plugin, or later
    # using the serialize_attributes class method.
    #
    # Because of how this plugin works, it must be used inside each model class
    # that needs serialization, after any set_dataset method calls in that class.
    # Otherwise, it is possible that the default column accessors will take
    # precedence.
    #
    # == Example
    #
    #   require 'sequel'
    #   # Require json, as the plugin doesn't require it for you.
    #   require 'json'
    #
    #   # Register custom serializer/deserializer pair
    #   Sequel::Plugins::Serialization.register_format(:reverse,
    #     lambda{|v| v.reverse},
    #     lambda{|v| v.reverse})
    #
    #   class User < Sequel::Model
    #     # Built-in format support when loading the plugin
    #     plugin :serialization, :json, :permissions
    #
    #     # Built-in format support after loading the plugin using serialize_attributes
    #     plugin :serialization
    #     serialize_attributes :marshal, :permissions
    #
    #     # Use custom registered serialization format just like built-in format
    #     serialize_attributes :reverse, :password
    #
    #     # Use a custom serializer/deserializer pair without registering
    #     serialize_attributes [lambda{|v| v.reverse}, lambda{|v| v.reverse}], :password
    #   end
    #   user = User.create
    #   user.permissions = { :global => 'read-only' }
    #   user.save
    module Serialization
      # The default serializers supported by the serialization module.
      # Use register_format to add serializers to this hash.
      REGISTERED_FORMATS = {}

      # Set up the column readers to do deserialization and the column writers
      # to save the value in deserialized_values.
      def self.apply(model, *args)
        model.instance_eval do
          @deserialization_map = {}
          @serialization_map = {}
        end
      end
      
      # Automatically call serialize_attributes with the format and columns unless
      # no columns were provided.
      def self.configure(model, format=nil, *columns)
        model.serialize_attributes(format, *columns) unless columns.empty?
      end

      # Register a serializer/deserializer pair with a format symbol, to allow
      # models to pick this format by name.  Both serializer and deserializer
      # should be callable objects.
      def self.register_format(format, serializer, deserializer)
        REGISTERED_FORMATS[format] = [serializer, deserializer]
      end
      register_format(:marshal, lambda{|v| [Marshal.dump(v)].pack('m')}, lambda{|v| Marshal.load(v.unpack('m')[0]) rescue Marshal.load(v)})
      register_format(:yaml, lambda{|v| v.to_yaml}, lambda{|v| YAML.load(v)})
      register_format(:json, lambda{|v| v.to_json}, lambda{|v| JSON.parse(v)})

      module ClassMethods
        # A hash with column name symbols and callable values, with the value
        # called to deserialize the column.
        attr_reader :deserialization_map

        # A hash with column name symbols and callable values, with the value
        # called to serialize the column.
        attr_reader :serialization_map

        # Module to store the serialized column accessor methods, so they can
        # call be overridden and call super to get the serialization behavior
        attr_accessor :serialization_module

        # Copy the serialization_map and deserialization map into the subclass.
        def inherited(subclass)
          super
          sm = serialization_map.dup
          dsm = deserialization_map.dup
          subclass.instance_eval do
            @deserialization_map = dsm
            @serialization_map = sm
          end
        end
        
        # Create instance level reader that deserializes column values on request,
        # and instance level writer that stores new deserialized values.
        def serialize_attributes(format, *columns)
          if format.is_a?(Symbol)
            unless format = REGISTERED_FORMATS[format]
              raise(Error, "Unsupported serialization format: #{format} (valid formats: #{REGISTERED_FORMATS.keys.map{|k| k.inspect}.join})")
            end
          end
          serializer, deserializer = format
          raise(Error, "No columns given.  The serialization plugin requires you specify which columns to serialize") if columns.empty?
          define_serialized_attribute_accessor(serializer, deserializer, *columns)
        end
        
        # The columns that will be serialized.  This is only for
        # backwards compatibility, use serialization_map in new code.
        def serialized_columns
          serialization_map.keys
        end

        private

        # Add serializated attribute acessor methods to the serialization_module
        def define_serialized_attribute_accessor(serializer, deserializer, *columns)
          m = self
          include(self.serialization_module ||= Module.new) unless serialization_module
          serialization_module.class_eval do
            columns.each do |column|
              m.serialization_map[column] = serializer
              m.deserialization_map[column] = deserializer
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
        def initialize_set(values)
          @deserialized_values = {}
          super
        end

        # Serialize all deserialized values
        def before_save
          deserialized_values.each{|k,v| @values[k] = serialize_value(k, v)}
          super
        end
        
        # Initialization the deserialized values for objects retrieved from the database.
        def set_values(*)
          @deserialized_values ||= {}
          super
        end

        private

        # Empty the deserialized values when refreshing.
        def _refresh(*)
          @deserialized_values = {}
          super
        end

        # Deserialize the column value.  Called when the model column accessor is called to
        # return a deserialized value.
        def deserialize_value(column, v)
          unless v.nil?
            raise Sequel::Error, "no entry in deserialization_map for #{column.inspect}" unless callable = model.deserialization_map[column]
            callable.call(v)
          end
        end

        # Serialize the column value.  Called before saving to ensure the serialized value
        # is saved in the database.
        def serialize_value(column, v)
          unless v.nil?
            raise Sequel::Error, "no entry in serialization_map for #{column.inspect}" unless callable = model.serialization_map[column]
            callable.call(v)
          end
        end
      end
    end
  end
end
