module Sequel
  module Plugins
    # SkipCreateRefresh is a simple plugin that make Sequel not
    # refresh after saving a new model object.  Sequel does the
    # refresh to make sure all columns are populated, which is
    # necessary so that database defaults work correctly.
    #
    # This plugin is mostly for performance reasons where you
    # want to save the cost of select statement after the insert,
    # but it could also help cases where records are not
    # immediately available for selection after insertion.
    #
    # Note that Sequel does not attempt to refresh records when
    # updating existing model objects, only when inserting new
    # model objects.
    # 
    # Usage:
    #
    #   # Make all model subclass instances skip refreshes when saving
    #   # (called before loading subclasses)
    #   Sequel::Model.plugin :skip_create_refresh
    #
    #   # Make the Album class skip refreshes when saving
    #   Album.plugin :skip_create_refresh
    module SkipCreateRefresh
      module InstanceMethods
        private
        # Do nothing instead of refreshing the record inside of save.
        def _save_refresh
          nil
        end
      end
    end
  end
end
