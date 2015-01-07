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
    #   # Require json if you plan to use it, as the plugin doesn't require it for you.
    #   require 'json'
    #
    #   # Register custom serializer/deserializer pair, if desired
    #   require 'sequel/plugins/serialization'
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
    #
    # Note that if you mutate serialized column values without reassigning them,
    # those changes won't be picked up by <tt>Model#save_changes</tt> or
    # <tt>Model#update</tt>.  Example:
    #
    #   user = User[1]
    #   user.permissions[:global] = 'foo'
    #   user.save_changes # Will not pick up changes to permissions
    #
    # You can use the +serialization_modification_detection+ plugin to pick
    # up such changes.
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
      register_format(:marshal, lambda{|v| [Marshal.dump(v)].pack('m')},
        lambda do |v|
          begin
            Marshal.load(v.unpack('m')[0])
          rescue => e
            begin
              # Backwards compatibility for unpacked marshal output.
              Marshal.load(v)
            rescue
              raise e
            end
          end
        end)
      register_format(:yaml, lambda{|v| v.to_yaml}, lambda{|v| YAML.load(v)})
      register_format(:json, lambda{|v| Sequel.object_to_json(v)}, lambda{|v| Sequel.parse_json(v)})

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

        Plugins.inherited_instance_variables(self, :@deserialization_map=>:dup, :@serialization_map=>:dup)
        
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
                elsif frozen?
                  deserialize_value(column, super())
                else
                  deserialized_values[column] = deserialize_value(column, super())
                end
              end
              define_method("#{column}=") do |v| 
                if !changed_columns.include?(column) && (new? || get_column_value(column) != v)
                  changed_columns << column
                end

                deserialized_values[column] = v
              end
            end
          end
        end
      end

      module InstanceMethods
        # Hash of deserialized values, used as a cache.
        def deserialized_values
          @deserialized_values ||= {}
        end

        # Freeze the deserialized values
        def freeze
          deserialized_values.freeze
          super
        end

        private

        # Serialize deserialized values before saving
        def _before_validation
          serialize_deserialized_values
          super
        end
        
        # Clear any cached deserialized values when doing a manual refresh.
        def _refresh_set_values(hash)
          @deserialized_values.clear if @deserialized_values
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

        # Dup the deserialized values when duping model instance.
        def initialize_copy(other)
          super
          @deserialized_values = other.deserialized_values.dup
          self
        end

        # Serialize all deserialized values
        def serialize_deserialized_values
          deserialized_values.each{|k,v| @values[k] = serialize_value(k, v)}
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
