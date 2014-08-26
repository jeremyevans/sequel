# The set_overrides extension adds the Dataset#set_overrides and
# Dataset#set_defaults methods which provide a crude way to
# control the values used in INSERT/UPDATE statements if a hash
# of values is passed to Dataset#insert or Dataset#update.
# It is only recommended to use this for backwards compatibility.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:set_overrides)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:set_overrides)

#
module Sequel
  module SetOverrides
    Dataset::NON_SQL_OPTIONS.concat([:defaults, :overrides])
    Dataset.def_mutation_method(:set_defaults, :set_overrides, :module=>self)

    # Set overrides/defaults for insert hashes
    def insert_sql(*values)
      if values.size == 1 && (vals = values.first).is_a?(Hash)
        super(merge_defaults_overrides(vals))
      else
        super
      end
    end

    # Set the default values for insert and update statements.  The values hash passed
    # to insert or update are merged into this hash, so any values in the hash passed
    # to insert or update will override values passed to this method.  
    #
    #   DB[:items].set_defaults(:a=>'a', :c=>'c').insert(:a=>'d', :b=>'b')
    #   # INSERT INTO items (a, c, b) VALUES ('d', 'c', 'b')
    def set_defaults(hash)
      clone(:defaults=>(@opts[:defaults]||{}).merge(hash))
    end

    # Set values that override hash arguments given to insert and update statements.
    # This hash is merged into the hash provided to insert or update, so values
    # will override any values given in the insert/update hashes.
    #
    #   DB[:items].set_overrides(:a=>'a', :c=>'c').insert(:a=>'d', :b=>'b')
    #   # INSERT INTO items (a, c, b) VALUES ('a', 'c', 'b')
    def set_overrides(hash)
      clone(:overrides=>hash.merge(@opts[:overrides]||{}))
    end

    # Set overrides/defaults for update hashes
    def update_sql(values = {})
      if values.is_a?(Hash)
        super(merge_defaults_overrides(values))
      else
        super
      end
    end

    private

    # Return new hashe with merged defaults and overrides.
    def merge_defaults_overrides(vals)
      vals = @opts[:defaults].merge(vals) if @opts[:defaults]
      vals = vals.merge(@opts[:overrides]) if @opts[:overrides]
      vals
    end
  end

  Dataset.register_extension(:set_overrides, SetOverrides)
end
