module Sequel
  module Plugins
    # The tactical_eager_loading plugin allows you to eagerly load
    # an association for all objects retreived from the same dataset
    # without calling eager on the dataset.  If you attempt to load
    # associated objects for a record and the association for that
    # object is currently not cached, it assumes you want to get
    # the associated objects for all objects retreived with the dataset that
    # retreived the current object.
    #
    # Tactical eager loading requires the identity_map plugin to
    # function correctly.  It only takes affect if you reteived the
    # current object with Dataset#all, it doesn't work if you
    # retreived the current object with Dataset#each.
    #
    # Basically, this allows the following code to issue only two queries:
    #
    #   Sequel::Model.with_identity_map do
    #     Album.filter{id<100}.all do |a|
    #       a.artists
    #     end
    #   end
    module TacticalEagerLoading
      module InstanceMethods
        # The dataset that retreived this object, set if the object was
        # reteived via Dataset#all with an active identity map.
        attr_accessor :retreived_by

        # All model objects retreived with this object, set if the object was
        # reteived via Dataset#all with an active identity map.
        attr_accessor :retreived_with

        private

        # If there is an active identity map and the association is not in the
        # associations cache and the object was reteived via Dataset#all,
        # eagerly load the association for all model objects retreived with the
        # current object.
        def load_associated_objects(opts, reload=false)
          name = opts[:name]
          if model.identity_map && !associations.include?(name) && retreived_by
            retreived_by.send(:eager_load, retreived_with, name=>{})
          end
          super
        end
      end

      module DatasetMethods
        private

        # If there is an active identity map, set the reteived_with attribute for the object
        # with the current dataset and array of all objects.
        def post_load(objects)
          super
          if model.identity_map
            objects.each do |o|
              o.retreived_by = self
              o.retreived_with = objects
            end
          end
        end
      end
    end
  end
end
