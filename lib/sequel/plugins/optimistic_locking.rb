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
    #   p1.update(:name=>'Jim') # works
    #   p2.update(:name=>'Bob') # raises Sequel::Plugins::OptimisticLocking::Error
    #
    # In order for this plugin to work, you need to make sure that the database
    # table has a lock_version column (or other column you name via the lock_column
    # class level accessor) that defaults to 0.
    #
    # This plugin does not work with the class_table_inheritance plugin.
    module OptimisticLocking
      # Exception class raised when trying to update or destroy a stale object.
      class Error < Sequel::Error
      end

      # Set the lock_column to the :lock_column option, or :lock_version if
      # that option is not given.
      def self.configure(model, opts={})
        model.lock_column = opts[:lock_column] || :lock_version
      end
      
      module ClassMethods
        # The column holding the version of the lock
        attr_accessor :lock_column
        
        # Copy the lock_column value into the subclass
        def inherited(subclass)
          super
          subclass.lock_column = lock_column
        end
      end
    
      module InstanceMethods
        private
        
        # Only delete the object when destroying if it has the same lock version.  If the row
        # doesn't have the same lock version, raise an error.
        def _destroy_delete
          lc = model.lock_column
          raise(Error, "Attempt to destroy a stale object") if this.filter(lc=>send(lc)).delete != 1
        end
        
        # Only update the row if it has the same lock version, and increment the
        # lock version.  If the row doesn't have the same lock version, raise
        # an Error.
        def _update(columns)
          lc = model.lock_column
          lcv = send(lc)
          columns[lc] = lcv + 1
          raise(Error, "Attempt to update a stale object") if this.filter(lc=>lcv).update(columns) != 1
          send("#{lc}=", lcv + 1)
        end
      end
    end
  end
end
