---
 layout: post
 title: Limited Eager Loading
---

Sequel, like most ruby ORMs, supports placing limits on associations:

    Album.one_to_many :first_10_tracks, :class=>:Track, :order=>:number, :limit=>10

With Sequel, just like most ruby ORMs, retrieving this association will correctly load only the first 10 associated objects:

    Album[1].first_10_tracks
    # SELECT * FROM "tracks" WHERE ("tracks"."album_id" = 1) ORDER BY "number" LIMIT 10

However, attempting to eagerly load a limited association didn't work correctly, with the limit being ignored:

    Album.filter(:id=>[1, 2]).eager(:first_10_tracks).all
    # SELECT * FROM "tracks" WHERE ("tracks"."album_id" IN (1, 2)) ORDER BY "number"

You can't use the LIMIT clause in such a situation, because it limits the total number of track rows, while you need the results limited per album_id.  

Up until recently, Sequel operating much like other ruby ORMs, ignoring the limit and returning all associated rows.  However, recent commits to Sequel have added support for multiple strategies for eagerly loading such associations correctly.

By default, Sequel will just do the limiting in ruby, by slicing the resulting array.  This is mainly for safety, as it uses the same query to load the associations and should not cause any backwards compatibility issues.  However, it doesn't offer better performance, as it stil loads all associated rows instead of just the limited ones.

Sequel supports two strategies that modify the query so that the database itself only returns rows limited to a given number per current object.  The first strategy is :window_function.  This uses the row_number window function partitioned by the related key column:

    Album.one_to_many :first_10_tracks, :class=>:Track, :order=>:number, :limit=>10, :eager_limit_strategy=>:window_function
    Album.filter(:id=>[1, 2]).eager(:first_10_tracks).all
    # SELECT * FROM (
    #   SELECT *, row_number() OVER (PARTITION BY "tracks"."album_id" ORDER BY "number") AS "x_sequel_row_number_x"
    #   FROM "tracks" WHERE ("tracks"."album_id" IN (1, 2))
    # ) AS "t1"
    # WHERE ("x_sequel_row_number_x" <= 10)

row_number usually produces a sequential list of numbers for all of the rows of the table, where the first row has row_number 1, the second has row_number 2, etc. However, when you partition row_number by a column, it generates it separate sequential list for each value of that column, so the first row with album_id 1 has row_number 1, the second row with album_id 1 has row_number 2, the first row with album_id 2 has row_number 1, etc.  This gives you the correct results and usually performs well.  However, it can only be used on databases that support window functions.  Most good databases do, with PostgreSQL supporting them since 8.4, Microsoft SQL Server supporting them since 2005, and DB2 supporing them since I'm not sure when.

If you stuck with a database that doesn't support window functions, Sequel can use a different strategy, named :correlated_subquery.  This uses a correlated subquery to correctly eagerly load the associated objects.  However, you need to be careful when using it, as a correlated subquery runs a separate query per row (internally to the database).

    Album.one_to_many :first_10_tracks, :class=>:Track, :order=>:number, :limit=>10, :eager_limit_strategy=>:correlated_subquery
    Album.filter(:id=>[1, 2]).eager(:first_10_tracks).all
    # SELECT * FROM "tracks" WHERE (("tracks"."album_id" IN (1, 2)) AND ("track"."id" IN (
    #   SELECT "t1"."id" FROM "tracks" AS "t1" WHERE ("t1"."album_id" = "tracks"."album_id") ORDER BY "number" LIMIT 10))) 

Here, you can see that in the subquery, we alias the tracks table to t1, but in the subquery's WHERE clause, we also reference tracks.album_id.  Since tracks is not used in the subquery itself (it must be referred to by the alias t1), it looks in the outer query.  There it finds the tracks table.  So for every matching row in the tracks table, it does in internal query to the tracks table to get the first 10 rows for that album.  If the row in the outer query is one of the first 10 rows in the subquery, it will be returned, otherwise it will be skipped.

A correlated subquery will almost always perform worse than a window function-based approach.  On PostgreSQL in some basic testing, using a window function approach was about 10 times faster.  But if you are using SQLite or another database that does not support window functions, it's the only strategy that works inside the database, and it can be significantly faster than lazy loading or eager loading all related objects and then slicing the array in ruby.

Unfortunately, due to bugs/limitations in DB2 and MySQL, the :correlated_subquery strategy doesn't work on those databases.  For DB2, this isn't a problem, as you would prefer to use the :window_function strategy anyway. Unfortunately MySQL doesn't support window functions, so on MySQL, you have to do the limiting in ruby.

Since Sequel targets multiple databases, there should be a way to pick the best available strategy.  You can pass an :eager_limit_strategy => true option, and Sequel will pick what it thinks is the best strategy.  Currently, it uses window functions if they are supported, and drops down to ruby array slicing if not.  This is because I suspect there are some cases where the :correlated_subquery strategy will become very slow.  For safety, you must manually choose to use the :correlated_subquery strategy by specifying it directly.

The examples shown here are for one_to_many with a simple key, but they also work for many_to_many and many_through_many, both with simple and composite keys.  Sequel also supports :eager_limit_strategy for one_to_one associations.  The reason for this is that one_to_one associations are often used where the actual database relationship is one_to_many, and :order is used to pick the first matching result.  Sequel supports :window_function and :correlated_subquery strategies for one_to_one, and it also supports a :distinct_on strategy, which is only usable on PostgreSQL.  It uses PostgreSQL's DISTINCT ON syntax, and is used by default on PostgreSQL as it performs better than using a window function.

    Album.one_to_one :first_track, :class=>:Track, :order=>:number, :eager_limit_strategy=>:distinct_on
    Album.filter(:id=>[1, 2]).eager(:first_track).all
    # SELECT DISTINCT ON ("track"."musicid") * FROM "track"
    # WHERE ("track"."musicid" IN (1, 2)) ORDER BY "track"."musicid", "number"

When using DISTINCT ON, you specify the columns that should be distinct and you must also order first by those columns.  Usually, you order by other columns, and PostgreSQL will return the first matching row per DISTINCT ON column.

With these changes, Sequel is the only ruby ORM to correctly support eager loading of limited associations.  Please let me know if you have questions about this new feature.
