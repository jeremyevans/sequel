# frozen-string-literal: true

module Sequel
  module Plugins
    # The validate_associated_context plugin extends the validate_associated
    # plugin to work with the validation_contexts plugin. It ensures associated
    # objects are validated using the validation context of the current object.
    #
    # This plugin loads the validate_associated and validation_contexts plugins.
    # The associated model must also use the validation_contexts plugin in order
    # to access the validation context.
    #
    #   class Album < Sequel::Model
    #     plugin :validate_associated_context
    #     many_to_one :artist
    #
    #     def validate
    #       super
    #       validate_associated_object(model.association_reflection(:artist), artist) if artist
    #     end
    #   end
    #
    #   class Artist < Sequel::Model
    #     plugin :validation_contexts
    #     def validate
    #       super
    #       errors.add(:name, 'not present') if name.nil? && validation_context == :publish
    #     end
    #   end
    #
    #   album = Album.new(artist: Artist.new)
    #   album.valid?                               # => true
    #   album.valid?(validation_context: :publish) # => false
    module ValidateAssociatedContext
      # Depend on the validate_associated and validation_contexts plugins.
      def self.apply(model)
        model.plugin :validate_associated
        model.plugin :validation_contexts
      end

      module InstanceMethods
        private

        # Whether the associated object is valid, using the validation context
        # of the current object.
        def associated_object_valid?(obj)
          obj.valid?(:validation_context=>validation_context)
        end
      end
    end
  end
end
