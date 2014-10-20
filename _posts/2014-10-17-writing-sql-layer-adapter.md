---
 layout: post
 title: Writing The FoundationDB SQL Layer Adapter
---

_This is a guest post from Scott Dugas. Scott is a developer at FoundationDB, and wrote the adapter included in Sequel 4.15.0 that connects to [FoundationDB's SQL Layer](https://foundationdb.com/layers/sql)._

We've spent the last year writing a complete, ANSI-compliant SQL database on top of our Key-Value storage engine. Now that we've released the layer, we're looking to broaden support for the many ORMs and libraries available to the SQL community. Some of the early adopters were wisely using Sequel as their prefered ORM, so that sent me on the path to adding one for the SQL Layer. This was my first experience with both Sequel and the SQL Layer, so it was quite an adventure.

The Sequel community is fortunate to have adapters available to so many different databases. Have you ever wondered what is involved in writing one? Well, wonder no more: here's the story of how the SQL Layer adapter came to be.

## Injection

I started out expecting to have to write the SQL Layer adapter as an external adapter, since this is pretty common among other ORMs. While it made running the tests a little harder, doing so wasn't too hard, and mostly was the same as building an integrated ORM. All I had to do was push the new adapter protocol (`fdbsql`) onto `Sequel::Database::Adapters` and create the Databasae class in `sequel/adapters/fdbsql.rb`.

## Running Tests
This is basically the only difference between creating an adapter in Sequel versus an external adapter. Fortunately the [SQL Layer adapter for ActiveRecord](https://github.com/FoundationDB/sql-layer-adapter-activerecord), provided me with an example. Thanks to ruby's power you can get the path to any gem after requiring it:

    require 'sequel'
    SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path

Using that path I set up a rake file to run all the integration tests from Sequel against the SQL Layer.

I set `ENV[SEQUEL_INTEGRATION_URL]` to my localhost: `'fdbsql://user@localhost:15432/sequel_testing'` and then ran the tests. Naturally they all failed because I hadn't implemented anything, but that's where the fun happens.

## Get as many tests passing as possible without touching the SQL Layer

Of course the test failures started out being exceptions in the before and after blocks, and every test was failing because I hadn't implemented `DROP TABLE` and `CREATE TABLE`. But, in order to do that I needed a real connection, so, I installed [`ruby-pg`](https://bitbucket.org/ged/ruby-pg/wiki/Home) to interact with the the SQL Layer adapter via the Postgres protocol. Don't be fooled though, the SQL Layer may speak the Postgres protocol, but it has lots of differences from Postgres.

### SQL Functions

One of the first ones that I hit  was, something like: `PG::SyntaxErrorOrAccessRuleViolation: ERROR:  Encountered " "<<" "<< "" at line 1, column 13.`. Of course my first reaction was that the SQL Layer didn't have bitwise operators, but, it turns out that it does have the operations, but they're functions. The same goes for regular expressions. So instead of `a !~ b` it becomes `REGEX(a,b)`, which conveniently just required me to override `complex_expression_sql_append`, and for a set of operators use the functions instead. (There are quite a few `*_append` methods in Sequel). Unfortunately these errors can be tricky, because they look just like syntax/unsupported errors.

### Schema parsing

This was one of the trickier pieces of Sequel code, because it has to do multiple joins to get all the column information that shows up. I started out with a big string of SQL code, and then learned all of Sequels tricks to convert it into nice pretty ruby syntax like the shortcut for schemas and aliases: `scheme__column_name___alias`.

### Deal with retry stuff

So, one thing that differs about FoundationDB's transactions versus a lot of other popular SQL instances, is that occasionally, you get a variety of retryable errors, such as `past_version` (if the system is under too much load you may get this). As with the Sequel convention, these get bubbled up, but it is something that application developers will have to be aware of when using Sequel (or any other adapter) with the FoundationDB SQL Layer.

## Add Supports flags

Sequel has a whole bunch of `supports_*` methods that determine how Sequel will interact with the adapter, and which tests to run. The main challenge here was figuring out whether it was something we didn't support, or if it was just something where we had a different syntax (like the bitwise operators). One ended up being rather tricky, `uses_with_rollup?`, because when enabled the SQL Layer was just ignoring the `WITH ROLLUP` part of the statement, and giving incorrect results (this has been changed to throw an error, until it's supported). So, I guess the moral here, is that if you're going to implement an adapter it's a good idea to know what the database actually supports.

## Fix bugs

Now that I had all of Sequel's tests running, it started to expose some bugs. We have a pretty expansive test suite so we weren't expecting many. Some of these were slightly embarrassing: `select 1 where true is not true` was returning a single row with `1`, even though `select true is not true` correctly returned `false`. Others were more complicated: `SELECT * FROM "artists" WHERE (("artists"."id" NOT IN (SELECT "albums_artists"."artist_id" FROM "albums_artists" INNER JOIN "albums" ON ("albums"."id" = "albums_artists"."album_id") INNER JOIN "albums_artists" AS "albums_artists_0" ON ("albums_artists_0"."album_id" = "albums"."id") WHERE (("albums_artists_0"."artist_id" IN (1, 4)) AND ("albums_artists"."artist_id" IS NOT NULL)))) OR ("artists"."id" IS NULL))`. It appears that every adapter has its own little flavor of SQL, and Sequel is no exception.

## Patch Sequel tests

When all was said and done, and all the bugs were fixed, it left a few things that just aren't handled:

* The SQL Layer doesn't support temporary tables yet
* It doesn't support Constraint Checks
* Dropping the primary key constraint on a column that is a serial column throws an error

Not too shabby.

## Get it merged upstream

Once it was all set and nearly all the tests were passing, I prepped to merge it upstream. First, I added JDBC support, and reformatted to match the Sequel style. JDBC was fairly straightforward since the SQL Layer already has a JDBC driver. Most of the challenge was detecting which parts of the adapter were pg specific, and which should be shared, so I'd recommend deciding to do both or one upfront if you decide to add a new adapter. After that, I passed it off to Jeremy, who made a few more modifications to support other rubies and rspecs, and a couple more style changes.


## Summary

There were some definite hiccups along the way, but for the most part, adding the new adapter was fairly straightforward, and now you can get the fault tolerance and scalability that the SQL Layer provides with the ease of use and clean code that Sequel provides.


