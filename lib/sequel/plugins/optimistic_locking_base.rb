# frozen-string-literal: true

module Sequel
  module Plugins
    # Base for other optimistic locking plugins
    module OptimisticLockingBase
      # Load the instance_filters plugin into the model.
      def self.apply(model)
        model.plugin :instance_filters
      end

      module ClassMethods
        # The column holding the version of the lock
        attr_accessor :lock_column

        # Prevent increasing the value of the lock column. If the lock column
        # has an existing value, do not allow setting a value that is higher
        # than it.
        def prevent_lock_column_increase!
          # SEQUEL6: Make default, require explicit opt out to increase
          lc = lock_column
          define_method(:"#{lc}=") do |v|
            unless v && (lv = send(lc)) && v > lv
              super(v)
            end
          end
        end
        
        Plugins.inherited_instance_variables(self, :@lock_column=>nil)
      end
    
      module InstanceMethods
        # Add the lock column instance filter to the object before destroying it.
        def before_destroy
          lock_column_instance_filter
          super
        end
        
        # Add the lock column instance filter to the object before updating it.
        def before_update
          lock_column_instance_filter
          super
        end
        
        private
        
        # Add the lock column instance filter to the object.
        def lock_column_instance_filter
          instance_filter(model.lock_column=>lock_column_instance_filter_value)
        end

        # Use the current value of the lock column
        def lock_column_instance_filter_value
          public_send(model.lock_column)
        end

        # Clear the instance filters when refreshing, so that attempting to
        # refresh after a failed save removes the previous lock column filter
        # (the new one will be added before updating).
        def _refresh(ds)
          clear_instance_filters
          super
        end
      end
    end
  end
end

