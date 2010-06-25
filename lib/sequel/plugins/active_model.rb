require 'active_model'
module Sequel
  module Plugins
    # The ActiveModel plugin makes Sequel::Model objects
    # pass the ActiveModel::Lint tests, which should
    # hopefully mean full ActiveModel compliance.  This should
    # allow the full support of Sequel::Model objects in Rails 3.
    # This plugin requires active_model in order to use
    # ActiveModel::Naming.
    # 
    # Usage:
    #
    #   # Make all subclasses active_model compliant (called before loading subclasses)
    #   Sequel::Model.plugin :active_model
    #
    #   # Make the Album class active_model compliant
    #   Album.plugin :active_model
    module ActiveModel
      ClassMethods = ::ActiveModel::Naming

      module InstanceMethods
        # The default string to join composite primary keys with in to_param.
        DEFAULT_TO_PARAM_JOINER = '-'.freeze
      
        # Record that an object was destroyed, for later use by
        # destroyed?
        def after_destroy
          super
          @destroyed = true
        end
        
        # False if the object is new? or has been destroyed, true otherwise.
        def persisted?
          !new? && @destroyed != true
        end
        
        # An array of primary key values, or nil if the object is not persisted.
        def to_key
          if persisted?
            primary_key.is_a?(Symbol) ? [pk] : pk
          end
        end

        # With the ActiveModel plugin, Sequel model objects are already
        # compliant, so this returns self.
        def to_model
          self
        end
        
        # An string representing the object's primary key.  For composite
        # primary keys, joins them with to_param_joiner.
        def to_param
          if k = to_key
            k.join(to_param_joiner)
          end
        end
        
        private
        
        # The string to use to join composite primary key param strings.
        def to_param_joiner
          DEFAULT_TO_PARAM_JOINER
        end
      end
    end
  end
end
