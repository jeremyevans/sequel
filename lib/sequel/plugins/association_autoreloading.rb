module Sequel
  module Plugins
    # The AssociationAutoreloading plugin makes many_to_one association
    # accessor methods automatically reload the cached object whenever
    # the association's foreign key is modified:
    #
    #     Album.many_to_one :artists
    #     album = Album.first
    #     album.artist_id #=> 1
    #     album.artist # caches associated artist
    #     album.artist_id = 2
    #     album.artist # reloads associated artist
    #
    module AssociationAutoreloading
      module ClassMethods
        private

        # Create a setter method for +key+ in an anonymous module included
        # in the class that calls super and clears the cache for
        # the given array of associations.
        def create_autoreloading_association_setter(key, assocs)
          include(@autoreloading_associations_module ||= Module.new) unless @autoreloading_associations_module
          @autoreloading_associations_module.class_eval do
            unless method_defined?("#{key}=")
              define_method("#{key}=") do |v|
                o = send(key)
                super(v)
                assocs.each{|a| associations.delete(a)} if send(key) != o
              end
            end
          end
        end

        # For each of the foreign keys in the association, create
        # a setter method that will clear the association cache.
        def def_many_to_one(opts)
          super
          @autoreloading_associations ||= {}
          opts[:keys].each do |key|
            assocs = @autoreloading_associations[key] ||= []
            assocs << opts[:name]
            create_autoreloading_association_setter(key, assocs)
          end
        end
      end
    end
  end
end
