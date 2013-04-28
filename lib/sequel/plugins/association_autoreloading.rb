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
      def self.apply(model)
        model.instance_variable_set(:@autoreloading_associations, {})
      end

      module ClassMethods
        # Hash with column symbol keys and arrays of many_to_one
        # association symbols that should be cleared when the column
        # value changes.
        attr_reader :autoreloading_associations

        Plugins.inherited_instance_variables(self, :@autoreloading_associations=>:hash_dup)

        private

        # Add the association to the array of associations to clear for
        # each of the foreign key columns.
        def def_many_to_one(opts)
          super
          opts[:keys].each do |key|
            (@autoreloading_associations[key] ||= []) << opts[:name]
          end
        end
      end

      module InstanceMethods
        private

        # If a foreign key column value changes, clear the related
        # cached associations.
        def change_column_value(column, value)
          if assocs = model.autoreloading_associations[column]
            assocs.each{|a| associations.delete(a)}
          end
          super
        end
      end
    end
  end
end
