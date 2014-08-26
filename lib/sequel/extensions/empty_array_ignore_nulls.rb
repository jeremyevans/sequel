# This changes Sequel's literalization of IN/NOT IN with an empty
# array value to not return NULL even if one of the referenced
# columns is NULL:
#
#   DB[:test].where(:name=>[])
#   # SELECT * FROM test WHERE (1 = 0)
#   DB[:test].exclude(:name=>[])
#   # SELECT * FROM test WHERE (1 = 1)
#
# The default Sequel behavior is to respect NULLs, so that when
# name is NULL, the expression returns NULL.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:empty_array_ignore_nulls)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:empty_array_ignore_nulls)

#
module Sequel
  module EmptyArrayIgnoreNulls
    # Use a simple expression that is always true or false, never NULL.
    def empty_array_value(op, cols)
      {1 => ((op == :IN) ? 0 : 1)}
    end
    
  end

  Dataset.register_extension(:empty_array_ignore_nulls, EmptyArrayIgnoreNulls)
end
