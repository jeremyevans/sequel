# frozen-string-literal: true

require 'active_model'
module Sequel
  module Plugins
    # The ActiveModel plugin makes Sequel::Model objects
    # pass the ActiveModel::Lint tests, which should
    # hopefully mean full ActiveModel compliance.  This should
    # allow the full support of Sequel::Model objects in Rails 3+.
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
      # ActiveModel compliant error class
      class Errors < Sequel::Model::Errors
        # Add autovivification so that #[] always returns an array.
        def [](k)
          fetch(k){self[k] = []}
        end
      end

      module ClassMethods
        include ::ActiveModel::Naming
        
        # Class level cache for to_partial_path.
        def _to_partial_path
          @_to_partial_path ||= "#{underscore(pluralize(to_s))}/#{underscore(demodulize(to_s))}".freeze
        end
      end

      module InstanceMethods
        # The default string to join composite primary keys with in to_param.
        DEFAULT_TO_PARAM_JOINER = '-'.freeze
      
        # Record that an object was destroyed, for later use by
        # destroyed?
        def after_destroy
          super
          @destroyed = true
        end

        # Mark current instance as destroyed if the transaction in which this
        # instance is created is rolled back.
        def before_create
          db.after_rollback{@destroyed = true}
          super
        end

        # Return ::ActiveModel::Name instance for the class.
        def model_name
          model.model_name
        end

        # False if the object is new? or has been destroyed, true otherwise.
        def persisted?
          !new? && @destroyed != true
        end
        
        # An array of primary key values, or nil if the object is not persisted.
        def to_key
          if primary_key.is_a?(Symbol)
            [pk] if pk
          else
            pk if pk.all?
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
          if persisted? and k = to_key
            k.join(to_param_joiner)
          end
        end

        # Returns a string identifying the path associated with the object.
        def to_partial_path
          model._to_partial_path
        end
        
        private

        # Use ActiveModel compliant errors class.
        def errors_class
          Errors
        end
        
        # The string to use to join composite primary key param strings.
        def to_param_joiner
          DEFAULT_TO_PARAM_JOINER
        end
      end
    end
  end
end
