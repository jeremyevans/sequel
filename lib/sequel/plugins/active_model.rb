require 'active_model'
module Sequel
  module Plugins
    # The ActiveModel plugin makes Sequel::Model objects the
    # pass the ActiveModel::Lint tests, which should
    # hopefully mean full ActiveModel compliance.  This should
    # allow the full support of Sequel::Model objects in Rails 3.
    # This plugin requires active_model in order to use
    # ActiveModel::Naming.
    module ActiveModel
      ClassMethods = ::ActiveModel::Naming

      module InstanceMethods
        # Record that an object was destroyed, for later use by
        # destroyed?
        def after_destroy
          super
          @destroyed = true
        end
        
        # Whether the object was destroyed by destroy.  Not true
        # for objects that were deleted.
        def destroyed?
          @destroyed == true
        end
        
        # An alias for new?
        def new_record?
          new?
        end

        # With the ActiveModel plugin, Sequel model objects are already
        # compliant, so this returns self.
        def to_model
          self
        end
      end
    end
  end
end
