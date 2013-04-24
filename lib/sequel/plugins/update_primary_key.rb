module Sequel
  module Plugins
    # The update_primary_key plugin allows you to modify an object's
    # primary key and then save the record.  Sequel does not work
    # correctly with primary key modifications by default.  Sequel
    # is designed to work with surrogate primary keys that never need to be
    # modified, but this plugin makes it work correctly with natural
    # primary keys that may need to be modified. Example:
    #
    #   album = Album[1]
    #   album.id = 2
    #   album.save
    # 
    # Usage:
    #
    #   # Make all model subclasses support primary key updates
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :update_primary_key
    #
    #   # Make the Album class support primary key updates
    #   Album.plugin :update_primary_key
    module UpdatePrimaryKey
      module ClassMethods
        # Cache the pk_hash when loading records
        def call(h)
          r = super(h)
          r.pk_hash
          r
        end
      end

      module InstanceMethods
        # Clear the pk_hash and object dataset cache, and recache
        # the pk_hash
        def after_update
          super
          @pk_hash = nil
          pk_hash
        end

        # Cache the pk_hash instead of generating it every time
        def pk_hash
          if frozen?
            super
          else
            @pk_hash ||= super
          end
        end

        private

        # If the primary key column changes, clear related associations.
        def change_column_value(column, value)
          pk = primary_key
          clear_associations_using_primary_key if (pk.is_a?(Array) ? pk.include?(column) : pk == column)
          super
        end

        # Clear associations that are likely to be tied to the primary key.
        # Note that this currently can clear additional options that don't reference
        # the primary key (such as one_to_many columns referencing a column other than the
        # primary key).
        def clear_associations_using_primary_key
          associations.keys.each do |k|
            associations.delete(k) if model.association_reflection(k)[:type] != :many_to_one
          end
        end
      end
    end
  end
end
