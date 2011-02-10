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
        def def_many_to_one(opts)
          super
          @autoreloading_associations ||= {}
          include(@autoreloading_associations_module ||= Module.new) unless @autoreloading_associations_module
          opts[:keys].each do |k|
            assocs = @autoreloading_associations[k] ||= []
            assocs << opts[:name]
            @autoreloading_associations_module.class_eval do
              unless method_defined?("#{k}=")
                define_method("#{k}=") do |v|
                  super(v)
                  assocs.each{|a| associations.delete(a)}
                end
              end
            end
          end
        end
      end
    end
  end
end
