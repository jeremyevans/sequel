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
        def load(h)
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
          @pk_hash ||= super
        end
      end
    end
  end
end
