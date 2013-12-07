---
 layout: post
 title: DataObjects Versus Native Adapters
---

Sequel is currently the only ruby ORM that supports both DataObjects adapters (e.g. do_postgres) as well as the standard native adapters (e.g. pg).  I occassionally get asked if it's better to use Sequel's do adapter instead of the native adapters when connecting to SQLite, MySQL, or PostgreSQL.  In the past, I've recommended using the native adapters unless you are having problems with them, as Sequel's do adapter has some edge cases that the native adapters do not have.  However, I hadn't done any comparative benchmarking of the do adapter against the native adapters, until now.

Using my <a href="http://github.com/jeremyevans/simple_orm_benchmark">simple_orm_benchmark</a> tool, I benchmarked the performance of PostgreSQL, MySQL, and SQLite using both the native adapters and the do adapter, <a href="http://pastie.org/868313.txt">with some interesting results</a>.  Here are my conclusions:

* PostgreSQL: The do adapter is about the same speed or slower (7% faster to 25% slower) than the standard postgres adapter.
* MySQL: The do adapter is slightly faster (up to 37%) than the standard mysql adapter.
* SQLite: The do adapter is up to three times faster than the standard sqlite adapter.

For MySQL and SQLite, unless you are depending on some special typecasting in the native adapter (e.g. datetimes in SQLite, tinyints as booleans in MySQL), if performance is important, you should give the do adapter a try.  For PostgreSQL, it's probably better to just stick with the native adapter.

Notes:

* simple_orm_benchmark is fairly simple, and the tests are not very extensive, so there may be plenty of performance differences that are not tested for.  I'll happily accept more test cases, just fork the repository and send me pull requests.

* The original purpose of the benchmarking was to determine if changing the do adapter to use the Reader#each method would increase performance of fetching records, and it turns it out hurts performance, and that the current implementation is most optimal of the four I tried.

* The data shows that there is not a major performance difference between PostgreSQL and MySQL in most cases.  Note that MySQL is using MyISAM tables in this example, which ignore transactions.  I've heard that InnoDB is slower than MyISAM, so the fact PostgreSQL performs pretty much the same for the same transaction usage compared to MyISAM should hopefully show that performance is no longer a reason to choose MySQL over PostgreSQL.  For many years, performance and replication were the only significant technical advantages to choosing MySQL over PostgreSQL. Considering the present state of affairs and the fact that PostgreSQL is rapidly gaining replication features, I certainly hope that more people will start choosing PostgreSQL when they need an SQL database.  I plan on benchmarking PostgreSQL against both InnoDB and MyISAM, look for those results in a future blog post.
