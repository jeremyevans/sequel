require 'securerandom'

module Sequel
  module Plugins
    # The uuid plugin creates hooks that automatically create a uuid for every
    # instance.
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
        
        # If the object has accessor methods for the uuid field, and the uuid
        # value is nil or overwriting it is allowed, set the uuid.
        def set_uuid(uuid=nil)
          field = model.uuid_field
          meth = :"#{field}="
          if respond_to?(field) && respond_to?(meth) && (model.uuid_overwrite? || get_column_value(field).nil?)
            set_column_value(meth, uuid||SecureRandom.uuid)
          end
        end
      end
    end
  end
end
