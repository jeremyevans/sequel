# frozen-string-literal: true

module Sequel
  module Plugins
    # This plugin implements a simple database-independent locking mechanism
    # to ensure that concurrent updates do not override changes. This is
    # best implemented by a code example:
    # 
    #   class Person < Sequel::Model
    #     plugin :optimistic_locking
    #   end
    #   p1 = Person[1]
    #   p2 = Person[1]
    #   p1.update(name: 'Jim') # works
    #   p2.update(name: 'Bob') # raises Sequel::NoExistingObject
    #
    # In order for this plugin to work, you need to make sure that the database
    # table has a +lock_version+ column that defaults to 0. To change the column
    # used, provide a +:lock_column+ option when loading the plugin:
    #
    #     plugin :optimistic_locking, lock_column: :version
    #
    # This plugin relies on the instance_filters plugin.
    module OptimisticLocking
      # Exception class raised when trying to update or destroy a stale object.
      Error = Sequel::NoExistingObject
      
      def self.apply(model, opts=OPTS)
        model.plugin(:optimistic_locking_base)
      end

      # Set the lock column
      def self.configure(model, opts=OPTS)
        model.lock_column = opts[:lock_column] || model.lock_column || :lock_version
      end

      module InstanceMethods
        private
        
        # Only update the row if it has the same lock version, and increment the
        # lock version.
        def _update_columns(columns)
          lc = model.lock_column
          lcv = get_column_value(lc)
          columns[lc] = lcv + 1
          super
          set_column_value("#{lc}=", lcv + 1)
          changed_columns.delete(lc)
          nil
        end
      end
    end
  end
end
