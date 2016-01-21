# frozen-string-literal: true

module Sequel
  module Plugins
    # The tactical_eager_loading plugin allows you to eagerly load
    # an association for all objects retrieved from the same dataset
    # without calling eager on the dataset.  If you attempt to load
    # associated objects for a record and the association for that
    # object is currently not cached, it assumes you want to get
    # the associated objects for all objects retrieved with the dataset that
    # retrieved the current object.
    #
    # Tactical eager loading only takes affect if you retrieved the
    # current object with Dataset#all, it doesn't work if you
    # retrieved the current object with Dataset#each.
    #
    # Basically, this allows the following code to issue only two queries:
    #
    #   Album.filter{id<100}.all do |a|
    #     a.artists
    #   end
    # 
    # Usage:
    #
    #   # Make all model subclass instances use tactical eager loading (called before loading subclasses)
    #   Sequel::Model.plugin :tactical_eager_loading
    #
    #   # Make the Album class use tactical eager loading
    #   Album.plugin :tactical_eager_loading
    module TacticalEagerLoading
      module InstanceMethods
        # The dataset that retrieved this object, set if the object was
        # reteived via Dataset#all.
        attr_accessor :retrieved_by

        # All model objects retrieved with this object, set if the object was
        # reteived via Dataset#all.
        attr_accessor :retrieved_with

        # Remove retrieved_by and retrieved_with when marshalling.  retrieved_by
        # contains unmarshallable objects, and retrieved_with can be very large
        # and is not helpful without retrieved_by.
        def marshallable!
          @retrieved_by = nil
          @retrieved_with = nil
          super
        end

        private

        # If there the association is not in the associations cache and the object
        # was reteived via Dataset#all, eagerly load the association for all model
        # objects retrieved with the current object.
        def load_associated_objects(opts, reload=false)
          name = opts[:name]
          if !associations.include?(name) && retrieved_by && !frozen?
            begin
              retrieved_by.send(:eager_load, retrieved_with.reject(&:frozen?), name=>{})
            rescue Sequel::UndefinedAssociation
              # This can happen if class table inheritance is used and the association
              # is only defined in a subclass.  This particular instance can use the
              # association, but it can't be eagerly loaded as the parent class doesn't
              # have access to the association, and that's the class doing the eager loading.
              nil
            end
          end
          super
        end
      end

      module DatasetMethods
        private

        # Set the retrieved_with and retrieved_by attributes for the object
        # with the current dataset and array of all objects.
        def post_load(objects)
          super
          objects.each do |o|
            next unless o.is_a?(Sequel::Model)
            o.retrieved_by = self
            o.retrieved_with = objects
          end
        end
      end
    end
  end
end
