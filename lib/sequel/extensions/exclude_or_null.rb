# frozen-string-literal: true
#
# The exclude_or_null extension adds Dataset#exclude_or_null and
# Dataset#exclude_or_null_having.  These methods are similar to
# Dataset#exclude and Dataset#exclude_having, except that they
# will also exclude rows where the condition IS NULL.
#
#   DB[:table].exclude_or_null(foo: 1)
#   # SELECT * FROM table WHERE NOT coalesce((foo = 1), false)
#   
#   DB[:table].exclude_or_null{foo(bar) =~ 1}
#   # SELECT * FROM table HAVING NOT coalesce((foo(bar) = 1), false))
#   
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:exclude_or_null)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:exclude_or_null)
#
# Note, this extension works correctly on PostgreSQL, SQLite, MySQL,
# H2, and HSQLDB.  However, it does not work correctly on Microsoft SQL Server,
# Oracle, DB2, SQLAnywhere, or Derby.
#
# Related module: Sequel::ExcludeOrNull

#
module Sequel
  module ExcludeOrNull
    # Performs the inverse of Dataset#where, but also excludes rows where the given
    # condition IS NULL.
    #
    #   DB[:items].exclude_or_null(category: 'software')
    #   # SELECT * FROM items WHERE NOT coalesce((category = 'software'), false)
    #   
    #   DB[:items].exclude_or_null(category: 'software', id: 3)
    #   # SELECT * FROM items WHERE NOT coalesce(((category = 'software') AND (id = 3)), false)
    def exclude_or_null(*cond, &block)
      add_filter(:where, cond, :or_null, &block)
    end

    # The same as exclude_or_null, but affecting the HAVING clause instead of the
    # WHERE clause.
    #
    #   DB[:items].select_group(:name).exclude_or_null_having{count(name) < 2}
    #   # SELECT name FROM items GROUP BY name HAVING NOT coalesce((count(name) < 2), true)
    def exclude_or_null_having(*cond, &block)
      add_filter(:having, cond, :or_null, &block)
    end

    private

    # Recognize :or_null value for invert, returning an expression for
    # the invert of the condition or the condition being null.
    def _invert_filter(cond, invert)
      if invert == :or_null
        ~SQL::Function.new(:coalesce, cond, SQL::Constants::SQLFALSE)
      else
        super
      end
    end
  end

  Dataset.register_extension(:exclude_or_null, ExcludeOrNull)
end
