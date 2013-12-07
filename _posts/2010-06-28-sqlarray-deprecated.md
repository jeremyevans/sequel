---
 layout: post
 title: SQLArray Deprecated
---

<a href="http://github.com/jeremyevans/sequel/commit/f0bf091b842fb2dea17ddcd6da87ae39e5ffa805">I added the Array#sql_array method and Sequel::SQL::SQLArray class to Sequel in October 2008</a>.  The method and class were added in order to handle an IN operator using two columns with a value list consisting of two element value lists.  By default, Sequel treats an array of two element arrays as a conditions specifier, similar to a hash, but with a defined order and the ability to have duplicate keys:

    DB[:items].filter([[:id, 1], [:name, 'A']])
    # SELECT * FROM items WHERE ((id = 1) AND (name = 'A'))

While this generally works fine, it causes a problem when you try to use the IN operator for multiple columns:

    DB[:items].filter([:a, :b]=>[[1, 2], [3, 4]])
    # SELECT * FROM items WHERE ((a, b) IN ((1 = 2) AND (3 = 4)))

Here, you don't want the array of two element arrays to be a conditions specifier, you want it to be a simple value list.  Previously, you had to call sql_array on the array to specify that it was a value list:

    DB[:items].filter([:a, :b]=>[[1, 2], [3, 4]].sql_array)
    # SELECT * FROM items WHERE ((a, b) IN ((1, 2), (3, 4)))

Again, this worked fine, but it's a step the user shouldn't have to take.  When used with the IN operator, the right hand side should either be a dataset or a value list, it should never be a boolean (which is what a condition specifier yields).  <a href="http://github.com/jeremyevans/sequel/commit/65abb56798e9ba72be381636d0cb8b31e42fd1d5">Sequel now handles this situation correctly when possible</a>, but there are still cases where it could be handled incorrectly:

    DB[:a].filter('(a, b) IN ?', [[1, 2], [3, 4]])
    # SELECT * FROM a WHERE ((a, b) IN ((1 = 2) AND (3 = 4)))

In this case, Sequel doesn't know that you are using the array of two element arrays as the IN predicate value, since one of Sequel's principles is that it never attempts to parse SQL.  So in this case, you still need to have some sort of way of marking the array as a value list and not a condition specifier.  For backwards compatibility, you can still use sql_array to do this:

    DB[:a].filter('(a, b) IN ?', [[1, 2], [3, 4]].sql_array)
    # SELECT * FROM a WHERE ((a, b) IN ((1, 2), (3, 4)))

However, the sql_array method is now considered deprecated.  I deprecated it because SQL99 actually defines an array type, and it is not related to the IN value list.  In hindsight, the sql_array method name and SQLArray class name were both poorly chosen.  For backwards compatibility, both will continue to work until at least Sequel 4, which is not even currently on the radar.  However, going forward, new code should use the sql_value_list method and Sequel::SQL::ValueList class instead.

One minor difference between SQL::ValueList and SQL::SQLArray is that SQL::ValueList now descends from Array instead of SQL::Expression.  This should make no difference in practice, as you cannot use an IN value list anywhere else in SQL, and there is no need for it to be an SQL::Expression subclass.
