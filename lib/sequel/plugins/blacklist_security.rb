module Sequel
  module Plugins
    # The blacklist_security plugin contains blacklist-based support for
    # mass assignment, specifying which columns to not allow mass assignment for,
    # implicitly allowing mass assignment for columns not listed.  This is only
    # for backwards compatibility, it should not be used by new code.
    #
    # Usage:
    #
    #   # Make all model subclasses support the blacklist security features. 
    #   Sequel::Model.plugin :blacklist_security
    #
    #   # Make the Album class support the blacklist security features.
    #   Album.plugin :blacklist_security
    module BlacklistSecurity
      module ClassMethods
        # Which columns are specifically restricted in a call to set/update/new/etc.
        # (default: not set).  Some columns are restricted regardless of
        # this setting, such as the primary key column and columns in Model::RESTRICTED_SETTER_METHODS.
        attr_reader :restricted_columns
  
        # Set the columns to restrict when using mass assignment (e.g. +set+).  Using this means that
        # attempts to call setter methods for the columns listed here will cause an
        # exception or be silently skipped (based on the +strict_param_setting+ setting).
        # If you have any virtual setter methods (methods that end in =) that you
        # want not to be used during mass assignment, they need to be listed here as well (without the =).
        #
        # It's generally a bad idea to rely on a blacklist approach for security.  Using a whitelist
        # approach such as set_allowed_columns or the instance level set_only or set_fields methods
        # is usually a better choice.  So use of this method is generally a bad idea.
        #
        #   Artist.set_restricted_columns(:records_sold)
        #   Artist.set(:name=>'Bob', :hometown=>'Sactown') # No Error
        #   Artist.set(:name=>'Bob', :records_sold=>30000) # Error
        def set_restricted_columns(*cols)
          clear_setter_methods_cache
          @restricted_columns = cols
        end
      end

      module InstanceMethods
        # Set all values using the entries in the hash, except for the keys
        # given in except.  You should probably use +set_fields+ or +set_only+
        # instead of this method, as blacklist approaches to security are a bad idea.
        #
        #   artist.set_except({:name=>'Jim'}, :hometown)
        #   artist.name # => 'Jim'
        def set_except(hash, *except)
          set_restricted(hash, false, except.flatten)
        end
    
        # Update all values using the entries in the hash, except for the keys
        # given in except.  You should probably use +update_fields+ or +update_only+
        # instead of this method, as blacklist approaches to security are a bad idea.
        #
        #   artist.update_except({:name=>'Jim'}, :hometown) # UPDATE artists SET name = 'Jim' WHERE (id = 1)
        def update_except(hash, *except)
          update_restricted(hash, false, except.flatten)
        end
      end
    end
  end
end
