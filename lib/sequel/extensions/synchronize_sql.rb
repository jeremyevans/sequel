# frozen-string-literal: true
#
# The synchronize_sql extension exists to work around some connection- pool
# performance considerations in a number of adapters where escaping a string
# as part of placeholder substitution requires an actual database connection.
# These adapters include amalgalite, mysql2, postgres, tinytds, and JDBC
# postgres. In these adapters, `literal_string_append` includes a call to
# `db.synchronize` to obtain a real connection object do use for escaping the
# passed-in string.
#
# This has the effect of checking out a connection from the pool for _every_
# placeholder that needs substitution in a query. For queries with lots of
# placeholders (e.g. IN queries with long lists), this can cause the query to
# spend longer waiting for a connection than the actual pool timeout (since
# every individual acquisition will take less than the timeout, but the sum of
# all of them can be greater)
#
# This dataset wraps all the _sql methods with a connection acquisition, so
# there will be no need to checkout/return connections continuously during the
# placeholder substitution.
#
# While this extension might solve some problems for heavily-loaded connection
# pools with queries containing lots of placeholders, it also creates some new
# ones. Now _every_ query is going to call `synchronize` twice, even queries
# that have no actual placeholders, and the total time spent holding a
# connection is going to be higher, since it's not released during the parts
# of query preparation that are not placeholder substitution.


module Sequel
    class Dataset
        module SynchronizeSQL
            %w(
                insert_sql
                select_sql
                update_sql
                delete_sql
            ).each do |method_name|
                define_method(method_name) do |*args|
                    db.synchronize(@opts[:server]) do
                        super *args
                    end
                end
            end
        end
        register_extension(:synchronize_sql, SynchronizeSQL)
    end
end
