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
    module TacticalEagerLoading
      module InstanceMethods
        # The dataset that retrieved this object, set if the object was
        # reteived via Dataset#all with an active identity map.
        attr_accessor :retrieved_by

        # All model objects retrieved with this object, set if the object was
        # reteived via Dataset#all with an active identity map.
        attr_accessor :retrieved_with

        private

        # If there is an active identity map and the association is not in the
        # associations cache and the object was reteived via Dataset#all,
        # eagerly load the association for all model objects retrieved with the
        # current object.
        def load_associated_objects(opts, reload=false)
          name = opts[:name]
          if !associations.include?(name) && retrieved_by
            retrieved_by.send(:eager_load, retrieved_with, name=>{})
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
