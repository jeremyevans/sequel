module Sequel
  module Plugins
    # The update_refresh plugin makes the model class refresh
    # the object after updating.  By default, Sequel only
    # refreshes automatically after inserting new rows, not
    # after updating.  However, if you are using triggers
    # to modify the contents of updated rows, it can be
    # helpful to immediately get the current data after
    # updating.
    #
    # If the dataset supports UPDATE RETURNING, this
    # plugin will use it so that it can retrieve the current
    # data in the same query it uses for the update.
    #
    # Usage:
    #
    #   # Make all model subclasses refresh after update
    #   Sequel::Model.plugin :update_refresh
    #
    #   # Make the Album class refresh after update
    #   Album.plugin :update_refresh
    module UpdateRefresh
      module InstanceMethods
        def after_update
          super
          unless this.supports_returning?(:update)
            refresh
          end
        end

        private

        def _update_without_checking(columns)
          ds = _update_dataset
          if ds.supports_returning?(:update)
            ds = ds.opts[:returning] ? ds : ds.returning
            rows = ds.update(columns)
            n = rows.length
            if n == 1
              @values.merge!(rows.first)
            end
            n
          else
            super
          end
        end
      end
    end
  end
end
