module Sequel
  module Plugins
    # The timestamps plugin creates hooks that automatically set create and
    # update timestamp fields.  Both field names used are configurable, and you 
    # can also set whether to overwrite existing create timestamps (false
    # by default), or whether to set the update timestamp when creating (also
    # false by default).
    # 
    # Usage:
    #
    #   # Timestamp all model instances using +created_at+ and +updated_at+
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :timestamps
    #
    #   # Timestamp Album instances, with custom column names
    #   Album.plugin :timestamps, :create=>:created_on, :update=>:updated_on
    #
    #   # Timestamp Artist instances, forcing an overwrite of the create
    #   # timestamp, and setting the update timestamp when creating
    #   Album.plugin :timestamps, :force=>true, :update_on_create=>true
    module Timestamps
      # Configure the plugin by setting the available options.  Note that
      # if this method is run more than once, previous settings are ignored,
      # and it will just use the settings given or the default settings.  Options:
      # :create :: The field to hold the create timestamp (default: :created_at)
      # :force :: Whether to overwrite an existing create timestamp (default: false)
      # :update :: The field to hold the update timestamp (default: :updated_at)
      # :update_on_create :: Whether to set the update timestamp to the create timestamp when creating (default: false)
      def self.configure(model, opts=OPTS)
        model.instance_eval do
          @create_timestamp_field = opts[:create]||:created_at
          @update_timestamp_field = opts[:update]||:updated_at
          @create_timestamp_overwrite = opts[:force]||false
          @set_update_timestamp_on_create = opts[:update_on_create]||false
        end
      end

      module ClassMethods
        # The field to store the create timestamp
        attr_reader :create_timestamp_field

        # The field to store the update timestamp
        attr_reader :update_timestamp_field
        
        # Whether to overwrite the create timestamp if it already exists
        def create_timestamp_overwrite?
          @create_timestamp_overwrite
        end
        
        Plugins.inherited_instance_variables(self, :@create_timestamp_field=>nil, :@update_timestamp_field=>nil,
          :@create_timestamp_overwrite=>nil, :@set_update_timestamp_on_create=>nil)
        
        # Whether to set the update timestamp to the create timestamp when creating
        def set_update_timestamp_on_create?
          @set_update_timestamp_on_create
        end
      end

      module InstanceMethods
        # Set the update timestamp when updating
        def before_update
          set_update_timestamp
          super
        end
        
        private
        
        # Set the create timestamp when creating
        def _before_validation
          set_create_timestamp if new?
          super
        end
        
        # If the object has accessor methods for the create timestamp field, and
        # the create timestamp value is nil or overwriting it is allowed, set the
        # create timestamp field to the time given or the current time.  If setting
        # the update timestamp on creation is configured, set the update timestamp
        # as well.
        def set_create_timestamp(time=nil)
          field = model.create_timestamp_field
          meth = :"#{field}="
          set_column_value(meth, time||=model.dataset.current_datetime) if respond_to?(field) && respond_to?(meth) && (model.create_timestamp_overwrite? || get_column_value(field).nil?)
          set_update_timestamp(time) if model.set_update_timestamp_on_create?
        end
        
        # Set the update timestamp to the time given or the current time if the
        # object has a setter method for the update timestamp field.
        def set_update_timestamp(time=nil)
          meth = :"#{model.update_timestamp_field}="
          set_column_value(meth, time||model.dataset.current_datetime) if respond_to?(meth)
        end
      end
    end
  end
end
