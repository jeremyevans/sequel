module Sequel
  module Plugins
    # The identity_map plugin allows the user to create temporary identity maps
    # via the with_identity_map method, which takes a block.  Inside the block,
    # objects have a 1-1 correspondence with rows in the database.
    # 
    # For example, the following is true, and wouldn't be true if you weren't
    # using the identity map:
    #   Sequel::Model.with_identity_map do
    #     Album.filter{(id > 0) & (id < 2)}.first.object_id == Album.first(:id=>1).object_id
    #   end
    #
    # In additional to providing a 1-1 correspondence, the identity_map plugin
    # also provides a cached looked up of records in two cases:
    # * Model.[] (e.g. Album[1])
    # * Model.many_to_one accessor methods (e.g. album.artist)
    #
    # If the object you are looking up using one of those two methods is already
    # in the identity map, the record is returned without a database query being
    # issued.
    #
    # Identity maps are thread-local and only presist for the duration of the block,
    # so they should be should only be considered as a possible performance enhancer.
    module IdentityMap
      module ClassMethods
        # Returns the current thread-local identity map.  Should be a hash if
        # there is an active identity map, and nil otherwise.
        def identity_map
          Thread.current[:sequel_identity_map]
        end
        
        # The identity map key for an object of the current class with the given pk.
        # May not always be correct for a class which uses STI.
        def identity_map_key(pk)
          "#{self}:#{pk ? Array(pk).join(',') : "nil:#{rand}"}"
        end
        
        # If the identity map is in use, check it for a current copy of the object.
        # If a copy does not exist, create a new object and add it to the identity map.
        # If a copy exists, add any values in the given row that aren't currently
        # in the object to the object's values.  This allows you to only request
        # certain fields in an initial query, make modifications to some of those
        # fields and request other, potentially overlapping fields in a new query,
        # and not have the second query override fields you modified.
        def load(row)
          return super unless idm = identity_map
          if o = idm[identity_map_key(Array(primary_key).map{|x| row[x]})]
            o.merge_db_update(row)
          else
            o = super
            idm[identity_map_key(o.pk)] = o
          end
          o
        end
        
        # Take a block and inside that block use an identity map to ensure a 1-1
        # correspondence of objects to the database row they represent.
        def with_identity_map
          return yield if identity_map
          begin
            self.identity_map = {}
            yield
          ensure
            self.identity_map = nil
          end
        end
        
        private

        # Set the thread local identity map to the given value. 
        def identity_map=(v) 
          Thread.current[:sequel_identity_map] = v
        end
        
        # Check the current identity map if it exists for the object with
        # the matching pk.  If one is found, return it, otherwise call super.
        def primary_key_lookup(pk)
          (idm = identity_map and o = idm[identity_map_key(pk)]) ? o : super
        end
      end

      module InstanceMethods
        # Remove instances from the identity map cache if they are deleted.
        def delete
          super
          if idm = model.identity_map
            idm.delete(model.identity_map_key(pk))
          end
          self
        end

        # Merge the current values into the values provided in the row, ensuring
        # that current values are not overridden by new values.
        def merge_db_update(row)
          @values = row.merge(@values)
        end

        private
        
        # If the association is a many_to_one and it has a :key option and the
        # key option has a value and the association uses the primary key of
        # the associated class as the :primary_key option, check the identity
        # map for the associated object and return it if present.
        def _load_associated_objects(opts)
          klass = opts.associated_class
          if idm = model.identity_map and opts[:type] == :many_to_one and opts[:primary_key] == klass.primary_key and
           opts[:key] and pk = send(opts[:key]) and o = idm[klass.identity_map_key(pk)]
            o
          else
            super
          end
        end
      end
    end
  end
end
