# frozen-string-literal: true

module Sequel
  module Plugins
    # The unlimited_update plugin is designed to work around a
    # MySQL warning in replicated environments, which occurs if
    # you issue an UPDATE with a LIMIT clause.  No other
    # database Sequel supports will create an UPDATE clause with
    # a LIMIT, and in non-replicated MySQL environments, MySQL
    # doesn't issue a warning.  Note that even in replicated
    # environments the MySQL warning is harmless, as Sequel
    # restricts an update to rows with a matching primary key,
    # which should be unique.
    #
    # Usage:
    #
    #   # Make all model subclass not use a limit for update
    #   Sequel::Model.plugin :unlimited_update
    #
    #   # Make the Album class not use a limit for update
    #   Album.plugin :unlimited_update
    module UnlimitedUpdate
      module InstanceMethods
        private

        # Use an unlimited dataset for updates.
        def _update_dataset
          super.unlimited
        end
      end
    end
  end
end
