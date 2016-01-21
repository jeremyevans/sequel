# frozen-string-literal: true

require 'securerandom'

module Sequel
  module Plugins
    # The uuid plugin creates hooks that automatically create a uuid for every
    # instance.  Note that this uses SecureRandom.uuid to create UUIDs, and
    # that method is not defined on ruby 1.8.7.  If you would like to use this
    # on ruby 1.8.7, you need to override the Model#create_uuid private method
    # to return a valid uuid.
    # 
    # Usage:
    #
    #   # Uuid all model instances using +uuid+
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :uuid
    #
    #   # Uuid Album instances, with custom column name
    #   Album.plugin :uuid, :field=>my_uuid
    module Uuid
      # Configure the plugin by setting the available options.  Note that
      # if this method is run more than once, previous settings are ignored,
      # and it will just use the settings given or the default settings.  Options:
      # :field :: The field to hold the uuid (default: :uuid)
      # :force :: Whether to overwrite an existing uuid (default: false)
      def self.configure(model, opts=OPTS)
        model.instance_eval do
          @uuid_field = opts[:field]||:uuid
          @uuid_overwrite = opts[:force]||false
        end
      end

      module ClassMethods
        # The field to store the uuid
        attr_reader :uuid_field
        
        # Whether to overwrite the create uuid if it already exists
        def uuid_overwrite?
          @uuid_overwrite
        end
        
        Plugins.inherited_instance_variables(self, :@uuid_field=>nil, :@uuid_overwrite=>nil)
      end

      module InstanceMethods
        private
        
        # Set the uuid when creating
        def _before_validation
          set_uuid if new?
          super
        end

        # Create a new UUID.  This method can be overridden to use a separate
        # method for creating UUIDs.  Note that this method does not work on
        # ruby 1.8.7, you will have to override it if you are using ruby 1.8.7.
        def create_uuid
          SecureRandom.uuid
        end
        
        # If the object has accessor methods for the uuid field, and the uuid
        # value is nil or overwriting it is allowed, set the uuid.
        def set_uuid(uuid=create_uuid)
          field = model.uuid_field
          meth = :"#{field}="
          if respond_to?(field) && respond_to?(meth) && (model.uuid_overwrite? || get_column_value(field).nil?)
            set_column_value(meth, uuid)
          end
        end
      end
    end
  end
end
