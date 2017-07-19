# frozen-string-literal: true

module Sequel
  module Plugins
    # The tactical_eager_loading plugin allows you to eagerly load
    # an association for all objects retrieved from the same dataset
    # without calling +eager+ on the dataset.  If you attempt to load
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
    #   Album.where{id<100}.all do |a|
    #     a.artists
    #   end
    #   # SELECT * FROM albums WHERE (id < 100)
    #   # SELECT * FROM artists WHERE id IN (...)
    #
    # Note that if you are passing a callback to the association method via
    # a block or :callback option, or using the :reload option to reload
    # the association, eager loading will not be done.
    #
    # You can use the :eager_reload option to reload the association for all
    # objects that the current object was retrieved with:
    #
    #   # SELECT * FROM albums WHERE (id < 100)
    #   albums = Album.where{id<100}.all
    #
    #   # Eagerly load all artists for these albums
    #   # SELECT * FROM artists WHERE id IN (...)
    #   albums.first.artists
    #
    #   # Do something that may affect which artists are associated to the albums
    #
    #   # Eagerly reload all artists for these albums
    #   # SELECT * FROM artists WHERE id IN (...)
    #   albums.first.artists(eager_reload: true)
    # 
    # You can also use the :eager option to specify dependent associations
    # to eager load:
    #
    #  albums = Album.where{id<100}.all
    #
    #   # Eager load all artists for these albums, and all albums for those artists
    #   # SELECT * FROM artists WHERE id IN (...)
    #   # SELECT * FROM albums WHERE artist_id IN (...)
    #   albums.first.artists(eager: :albums)
    #
    # You can also use :eager to specify an eager callback. For example:
    #
    #   albums = Album.where{id<100}.all
    #
    #   # Eagerly load all artists whose name starts with A-M for these albums
    #   # SELECT * FROM artists WHERE name > 'N' AND id IN (...)
    #   albums.first.artists(eager: lambda{|ds| ds.where(Sequel[:name] > 'N')})
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
        def load_associated_objects(opts, dynamic_opts=OPTS, &block)
          dynamic_opts = load_association_objects_options(dynamic_opts, &block)
          name = opts[:name]
          if (!associations.include?(name) || dynamic_opts[:eager_reload]) && retrieved_by && !frozen? && !dynamic_opts[:callback] && !dynamic_opts[:reload]
            begin
              retrieved_by.send(:eager_load, retrieved_with.reject(&:frozen?), name=>dynamic_opts[:eager] || {})
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
