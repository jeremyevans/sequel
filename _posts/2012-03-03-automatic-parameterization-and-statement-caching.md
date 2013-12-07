---
 layout: post
 title: Automatic Parameterization and Statement Caching
---

I recently committed some patches that add support for <a href="https://github.com/jeremyevans/sequel/commit/c467b8397420912969d75f3ab6589eb2e09a0d94">automatic parameterization of queries</a> and <a href="https://github.com/jeremyevans/sequel/commit/cbfa4e9315e900192ca6eab97af38b13cea6cd0a">caching and automatic preparation of statements</a> when Sequel's postgres adapter is used with the pg driver.  These extensions significantly change how Sequel generates and executes queries, and can improve performance in some cases.

### Historical Behavior

First, let's review Sequel's default behavior.  Unless you are <a href="http://sequel.jeremyevans.net/rdoc/files/doc/prepared_statements_rdoc.html">specifically choosing to use prepared statements</a>, Sequel by defaults literalizes all arguments directly into the SQL query string.  For example, this Sequel code:

    DB[:table].where(:a=>1, :b=>"c")

will use SQL similar to the following:

    SELECT * FROM table WHERE ((a = 1) AND (b = 'c'))

However, another way to execute this query would be to use placeholders for the values:

    SELECT * FROM table WHERE ((a = $1) AND (b = $2)) -- args [1, "c"]

Previously, you had to use Sequel's prepared statement API to use placeholders:

    DB[:table].where(:a=>:$a, :b=>:$b).call(:select, :a=>1, :b=>"c")

As that API is more cumbersome to use, it is used less frequently.

Historically, Sequel's internals have not been friendly to the usage of prepared statements.  <a href="https://github.com/jeremyevans/sequel/commit/78a683d97fb6a58cf8ed83777d097b49e8130056">Prepared statements and bound variables weren't supported at all until 2.4</a>, and as Sequel was not designed with the usage of bound variables/query parameters in mind, the internals were/are clunky, and the API for using them is suboptimal.  While I still think that the API chosen was the best one considering the existing codebase, I've always wished for a nicer way to implement them.

### Dataset Literalization Refactoring

With the <a href="https://github.com/jeremyevans/sequel/commit/092905dea17e1c800e5c6af6c38ff4997d0bdf8f">dataset literalization refactoring done between 3.29 and 3.30</a>, I finally had a nicer way.  Unlike <a href="https://github.com/rails/arel/blob/master/lib/arel/visitors/to_sql.rb">ActiveRecord/ARel</a>, Sequel has never used a separate visitor class to build an SQL string, it has done literalization directly in its dataset class, passing an SQL string to a bunch of query-building methods.  As Sequel datasets need to be thread-safe, the literalization process cannot modify the dataset in any way, and could only operate on the string, so there was no place to store any query parameters.  Before the dataset literalization refactoring, the string that represented the query wasn't necessarily a consistent object.  For example, Sequel used to do something similar the following to add the WHERE clause to the SQL string:

    sql << " WHERE #{literal(opts[:where])}"

So inside the literal method, there was no way to access the string being built for the query, or any way to pass the query parameters.

The dataset literalization refactoring changed this type of code to something similar to:

    sql << " WHERE "
    literal_append(sql, opts[:where])

So after the refactoring, all query building methods had access to the string being built.  However, while all the query methods had access to the string, there still was no place to store query parameters.  The usual way to handle this in ruby would be to store the query parameters in an instance variable, but since the instance is a dataset and datasets need to be thread-safe, you can't do that.  Even if you could work around the thread-safety issues, you'd have to have special code to deal with subselects (which could contain query parameters), and a whole bunch of other issues.

Thankfully, ruby is a flexible language, and it gives us a place to store the query parameters, even if it isn't obvious.  Instead of using a plain string to store the query, Sequel's pg\_auto\_parameterize extension uses a string subclass with an instance variable for storing the parameters.  Since a unique query string object is created each time a query needs to be built and this object is passed to all methods that are used to build the query, this works out well.  Sequel just needs to override a single method (literal\_append), so that instead of appending a literal version of the object into the query, it appends a placeholder to the query and adds the object as one of the query parameters.

### Literalization vs. Parameterization

Most ruby ORMs have historically used literalization as the primary if not the only way of executing queries.  While Sequel added support for parameterization with the bound variable/prepared statement API added in Sequel 2.4 in 2008, ActiveRecord didn't add support for parameterization at all until 3.1 in 2011.

In some other database communities, parameterization is thought of as the only correct way to do things, with literalization scoffed at and considered slow and less secure.  I don't necessarily agree that parameterization is better in all cases, though there are certainly use cases that benefit from it.

In my opinion, the main benefit of parameterization is not using parameters over literalization, but the fact that using parameters normalizes the SQL string being executed, allowing more opportunities for optimization.  For example, with the following Sequel code:

    DB[:table].where(:a=>1, :b=>"c")
    DB[:table].where(:a=>2, :b=>"d")

With literalization you get:

    SELECT * FROM table WHERE ((a = 1) AND (b = 'c'))
    SELECT * FROM table WHERE ((a = 2) AND (b = 'd'))

While with parameterization you get:

    SELECT * FROM table WHERE ((a = $1) AND (b = $2)) -- args [1, "c"]
    SELECT * FROM table WHERE ((a = $1) AND (b = $2)) -- args [2, "d"]

### Statement Caching and Automatic Preparation

As you may notice in the previous example, the same statement is used for both of the lines of Sequel code, with only the arguments differing.  Wouldn't it be great if we could use a prepared statement in that case, so the database doesn't have to parse and plan the same statement multiple times?

It turns out, that's pretty easy to do in the naive case.  You just use a hash keyed on the sql string, with the value being some object representing the prepared statement.  However, because the library doesn't know in advance which statements will be executed, you need to have some upper bound on the number of statements to prepare.  Also, when the cache is full, there needs to be some algorithm to determine which statements to keep in the cache and which statements to remove from the cache.  The most naive way to do that is just to keep the most recently executed statements in the cache, assuming that statements executed recently are most likely to be needed in the future, making it an LRU (least recently used) cache.  

Sequel's pg\_statement\_cache extension implements a less naive cache, which considers both the last time a query was executed and the number of times it has been executed.  The assumption that Sequel makes is that it is better to remove more recently executed statements that haven't been executed as frequently from the cache and to keep statements that have not been executed as recently but have been executed more frequently in the past.  Note that this is still a fairly naive algorithm.  A more intelligent algorithm would take into account the cost it takes to parse and plan the literalized version of the query, but there isn't a good way for Sequel to do that, and even if there was, doing so may cost more in computation time than the time saved.

Another issue when dealing with a query cache is deciding when to prepare the statements.  The most naive way to do this is just to prepare all queries to be executed, but this causes a performance hit when you only want to execute a query a single time.  It would be best to know in advance which queries need to be executed multiple times, and only prepare those queries, but automatic statement preparation generally cannot do that.  By default, Sequel chooses to prepare any query that needs to be executed multiple times.  So the first time Sequel sees a query, it will execute it normally, but the second time, it will prepare the statement and then execute it.

The other consideration when constructing the cache is deciding when to clean up the statements.  The most naive way to do this is that when you want to add an item to the cache and it is already full, just remove the least important item and insert the new item.  Unfortunately, depending on the algorithm and data structure used, determining the least important item can involve a fairly expensive computation.  In Sequel's case, this is true, as determining the least important item involves iterating over all of the items in the cache and running a computation on each item to determine the estimated value of the statements, sorting the statements by the resulting values, and removing the least valuable statements.  When the cache is full, doing this each time you wanted to insert an item would result in suboptimal performance.

Sequel uses a fairly simple design decision to lessen the performance impact of cleaning up the cache.  Instead of cleaning the cache every time you want to add an item, when it cleans the cache, it removes the least valuable half of the statements.  By default, Sequel sets the maximum size of the cache to 1000 statements, and when it cleans up the cache, it removes the 500 least valuable statements.  So it only has to take the performance hit of cleaning the cache occasionally, amortizing the at least O(n\*log(n)) cost of determining the estimated value of the statements over n/2 statements.  There are some trade offs with this method, though, in that if you have let's say 3n/4 important queries that are executed a lot, with a bunch of other queries that are only executed occasionally, when it comes time to clean the cache, it will probably remove about 1/3 of your important queries from the cache.

Because there is no one configuration of a statement cache that works well for all applications, Sequel allows multiple ways to configure the statement cache.  It allows you to set the maximum size of the cache as well as the minimum size of the cache, which is the size the cache should be after cleaning it.  The difference between the maximum and the minimum sizes is the number of statements to remove when cleaning the cache.  So in the above case where 3n/4 items are important, you would probably want a minimum size of at least 4n/5, so it wouldn't remove any of the important statements when cleaning the cache.  To avoid unnecessarily preparing statements that will not be used frequently, you can set the number of times to execute a statement normally before preparing it.  Finally, you can provide your own algorithm for assigning estimated values to statements, overriding the default.  The combination of these options should be enough to handle most use cases.

### Differences from ActiveRecord

<a href="https://github.com/rails/rails/blob/master/activerecord/lib/active_record/connection_adapters/postgresql_adapter.rb#L325">ActiveRecord 3.1+ appears to use automatic parameterization and statement caching similar to the new extensions I've added</a>.  ActiveRecord's statement cache uses a much simpler design than Sequel's.  When the cache reaches the maximum size, ActiveRecord <a href="https://github.com/rails/rails/blob/master/activerecord/lib/active_record/connection_adapters/postgresql_adapter.rb#L342">removes a statement using Hash#shift</a> (so I was wrong that the most naive way was an LRU cache).  This avoids a the need for a low water mark (the minimum pool size) and an possibly expensive algorithm to determine statement value, and honestly might be a better design in terms of performance.  It mostly depends on the cost of repreparing the same statement in PostgreSQL versus the cost of executing ruby code.  As the cost of preparing statements is very database and statement specific, it's hard to say which statement cache design is better, though certainly for simple databases and statements, I suspect ActiveRecord's is.

ActiveRecord's statement cache also differs from Sequel's in that it <a href="https://github.com/rails/rails/blob/master/activerecord/lib/active_record/connection_adapters/postgresql_adapter.rb#L1296">prepares all statements</a>, where Sequel by default prepares a statement only when it will be executed multiple times (though you can configure Sequel to prepare all queries).

As far as I know, ActiveRecord has not yet added a public API for creating and executing arbitrary prepared statements in a way similar to Sequel's prepared statement API (where the SQL is only built once and just executed repeatedly with different arguments).

### Caveats

Automatic parameterization on PostgreSQL currently uses type specifiers for all placeholders, guessing at which database type to use.  If it guesses incorrectly, PostgreSQL will complain and an exception will be raised when you attempt to execute the query.  These issues are generally simple to fix by adding an explicit cast yourself.

Automatic parameterization on PostgreSQL currently has almost no context, so it can't tell what a given ruby object really represents in SQL.  It assumes when you give it an integer, it represents a integer in the database.  However, you can do the following in SQL:

    SELECT id, name FROM table ORDER BY 1

In Sequel code, that would be:

    DB[:table].select(:id, :name).order(1)

Unfortunately, automatic parameterization will turn that into:

    SELECT id, name FROM table ORDER BY $1 -- args [1]

PostgreSQL complains about such a query, since you can't use query parameters to change how the query is ordered.  You can switch to using the column names or use a literal string to work around that issue.

Statement caching doesn't have many gotchas, except that if you modify a table used by a prepared statement in such a way that it changes the columns the query would return, PostgreSQL will raise an error.  As that's generally not done in production, it shouldn't be a major issue for users.

### Performance

What might surprise some people that aren't very familiar with PostgreSQL performance is that automatically parameterizing and caching statements is not necessary faster than Sequel's default literalization behavior, even if all you do is execute the statement over and over.  Here's some stupid benchmark results taken from a local database:

    
    # Literalization                                                       real
    SELECT * FROM table WHERE pk = integer LIMIT 1                     (  5.613807)
    SELECT * FROM table WHERE column = 118 character string LIMIT 1    (  6.045027)
    2 Table Join                                                       (  4.557464)
    3 Table Join                                                       (  4.638113)
    9 Table Join                                                       (  4.962573)

    # Automatic Parameterization                                           real
    SELECT * FROM table WHERE pk = integer LIMIT 1                     (  6.889978)
    SELECT * FROM table WHERE column = 118 character string LIMIT 1    (  6.964313)
    2 Table Join                                                       (  5.034440)
    3 Table Join                                                       (  5.258987)
    9 Table Join                                                       (  5.568579)

    # Automatic Parameterization and Statement Caching                     real
    SELECT * FROM table WHERE pk = integer LIMIT 1                     (  7.156773)
    SELECT * FROM table WHERE column = 118 character string LIMIT 1    (  7.375706)
    2 Table Join                                                       (  4.532341)
    3 Table Join                                                       (  3.063554)
    9 Table Join                                                       (  2.439051)

As one might expect, for more complex queries, there is a benefit, but for simple queries, performance is actually worse. Why is this?  Well, I haven't had time to fully analyze the reasons, but my guess is the overhead of automatically parameterizing and caching statements in Ruby exceeds the time saved in PostgreSQL.

It's not difficult to modify the pg\_auto\_parameterize extension so that only queries that join multiple tables (or meet some other arbitrary criteria) do automatic parameterization and statement caching.  However, optimizations like that are best applied on a per-application basis (the results you see here may not be reflective of a real world application).

For comparison, here are results using Sequel's prepared statement API:

    SELECT * FROM table WHERE pk = integer LIMIT 1                     (  3.172460)
    SELECT * FROM table WHERE column = 118 character string LIMIT 1    (  3.268584)
    2 Table Join                                                       (  2.882885)
    3 Table Join                                                       (  1.173159)
    9 Table Join                                                       (  1.753078)

It appears there are substantial benefits from preparing even simple queries in Sequel. I suspect this is because when using Sequel's prepared statement API, Sequel only has to create the SQL string once, and each time you call it,  Sequel more or less just passes the hash of arguments directly to the already prepared statement, so there is much less ruby code executed.

I'm not providing either the data or the benchmark code I used on purpose.  The only thing that should matter is how it performs on your application, and the only way to know that is to try it out yourself (and report in the comments).

### Conclusion

As currently implemented, automatic parameterization and statement caching does not necessarily improve performance. Using Sequel's prepared statement API manually seems to be fastest, but the prepared statement API is slightly more verbose and slightly less flexible.
