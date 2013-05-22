module Sequel
  module Plugins
    # The scissors plugin adds class methods for update, delete, and destroy.
    # It is so named because this is considered dangerous, since it is easy
    # to write:
    #
    #   Album.delete
    #
    # and delete all rows in the table, when you meant to write:
    #
    #   album.delete
    #
    # and only delete a single row.
    #
    # This plugin is mostly useful for backwards compatibility, and not
    # recommended for use in production.  However, it can cut down on
    # verbosity in non-transactional test code, so it may be appropriate
    # to use when testing.
    #
    # Usage:
    #
    #   # Make all model subclass run with scissors
    #   Sequel::Model.plugin :scissors
    #
    #   # Make the Album class run with scissors
    #   Album.plugin :scissors
    module Scissors
      module ClassMethods
        Plugins.def_dataset_methods(self, [:update, :delete, :destroy])
      end
    end
  end
end
