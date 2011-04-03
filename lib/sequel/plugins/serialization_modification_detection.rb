module Sequel
  module Plugins
    # Sequel's built in Serialization plugin doesn't check for modification
    # of the serialized objects, because it requires an extra deserialization of a potentially
    # very large object.  This plugin can detect changes in serialized values by
    # checking whether the current deserialized value is the same as the original
    # deserialized value.  This does require deserializing the value twice, but the
    # original deserialized value is cached.
    #
    # == Example
    #
    #   require 'sequel'
    #   require 'json'
    #   class User < Sequel::Model
    #     plugin :serialization, :json, :permissions
    #     plugin :serialization_modification_detection
    #   end
    #   user = User.create(:permissions => {})
    #   user.permissions[:global] = 'read-only'
    #   user.save_changes
    module SerializationModificationDetection
      # Load the serialization plugin automatically.
      def self.apply(model)
        model.plugin :serialization
      end
      
      module InstanceMethods
        # Detect which serialized columns have changed.
        def changed_columns
          cc = super
          deserialized_values.each{|c, v| cc << c if !cc.include?(c) && original_deserialized_value(c) != v} 
          cc
        end

        private

        # Clear the cache of original deserialized values after saving so that it doesn't
        # show the column is modified after saving.
        def after_save
          super
          @original_deserialized_values.clear if @original_deserialized_values
        end

        # Return the original deserialized value of the column, caching it to improve performance.
        def original_deserialized_value(column)
          (@original_deserialized_values ||= {})[column] ||= deserialize_value(column, self[column])
        end
      end
    end
  end
end
