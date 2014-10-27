---
 layout: post
 title: Writing The FoundationDB SQL Layer Adapter
---

_This is a guest post from Scott Dugas. Scott is a developer at FoundationDB, and wrote the adapter included in Sequel 4.15.0 that connects to [FoundationDB's SQL Layer](https://foundationdb.com/layers/sql)._

Earlier this year we released the SQL Layer for FoundationDB. For those of you who don't know, FoundationDB it is a transactional, distributed and fault tolerant key-value store. The SQL Layer inherits these properties while providing full ANSI-SQL database layered on top of it. The result is quite different from most RDBMS: a fault tolerant and highly scalable SQL database.

Now that we've released the SQL Layer, we're looking to broaden support for the many ORMs and libraries available to the SQL community. Some of the early adopters were wisely using Sequel as their preferred ORM, so that sent me on the path to adding one for the SQL Layer. This was my first experience with both Sequel and the SQL Layer, so it was quite the adventure.

The Sequel community is fortunate to already have adapters available to so many different databases. Have you ever wondered what is involved in writing one? Well, wonder no more: here's the story of how the SQL Layer adapter came to be.

## Running Tests

To get a new adapter in place, all I had to do was push the new adapter protocol (`fdbsql`) onto `Sequel::Database::Adapters` and create the Database class in `sequel/adapters/fdbsql.rb`.

I started out expecting to have to write the SQL Layer adapter as an external adapter, like some other Sequel adapters, and our adapters for other ORMs. The only difference between an internal adapter and external adapter turned out to be how the tests were run. For an external adapter I used `Gem.loaded_specs['sequel'].full_gem_path` in its Rakefile to get the path of the Sequel specs.

Just like any other adapter for Sequel, I set `ENV[SEQUEL_INTEGRATION_URL]` to my localhost: `'fdbsql://user@localhost:15432/sequel_testing'` and ran the tests. Naturally they all failed because I hadn't implemented anything, but that's where the fun starts.

## Get as many tests passing as possible without touching the SQL Layer

Of course every test was failing because I hadn't implemented `DROP TABLE` and `CREATE TABLE`. But, in order to do that I needed a real connection, so, I installed [`ruby-pg`](https://bitbucket.org/ged/ruby-pg/wiki/Home) to interact with the the SQL Layer adapter via the Postgres protocol. Don't be fooled though, the SQL Layer may speak the Postgres protocol, but it has lots of differences from Postgres, so my job was far from done.

One of the first errors that I hit  was, something like: `PG::SyntaxErrorOrAccessRuleViolation: ERROR:  Encountered " "<<" "<< "" at line 1, column 13.`. Of course my first reaction was that the SQL Layer didn't have bitwise operators, but, it turns out that it does have the functionality, but they're functions. The same goes for regular expressions. So instead of `a !~ b` it becomes `REGEX(a,b)`, which conveniently just required me to override `complex_expression_sql_append` (overriding `*_append` is a common idiom in Sequel). Unfortunately these errors can be tricky, because they look like unsupported errors, but they're actually just a different syntax.

Parsing the schema information was one of the trickier pieces of Sequel code, because it had to do multiple joins to get all the column information that Sequel wanted. I started out with a big string of SQL code, and then learned all of Sequel's tricks to convert it into nice pretty ruby syntax like the shortcut for schemas and aliases: `scheme__column_name___alias`.

Sequel has a whole bunch of `supports_*` methods that determine how Sequel will interact with the adapter, and which tests to run. So I had to go through each one and make sure that the SQL Layer overrode those as appropriate, which meant figuring out what it does support.

## Fix bugs

Now that I had all of Sequel's tests running, it started to expose some bugs. We have a pretty expansive test suite so we weren't expecting many. Some of these were slightly embarrassing: `select 1 where true is not true` was returning a single row with `1`, even though `select true is not true` correctly returned `false`. Others were more complicated:

    SELECT *
    FROM "artists"
    WHERE (("artists"."id" NOT IN
              (SELECT "albums_artists"."artist_id"
               FROM "albums_artists"
               INNER JOIN "albums" ON ("albums"."id" = "albums_artists"."album_id")
               INNER JOIN "albums_artists" AS "albums_artists_0" ON ("albums_artists_0"."album_id" = "albums"."id")
               WHERE (("albums_artists_0"."artist_id" IN (1, 4))
                      AND ("albums_artists"."artist_id" IS NOT NULL))))
           OR ("artists"."id" IS NULL))

It appears that every adapter has its own little flavor of SQL, and Sequel is no exception.

## Patch Sequel tests

When all was said and done, and all the bugs were fixed, it left a few things that just aren't handled:

* The SQL Layer doesn't support temporary tables yet
* It doesn't support Constraint Checks
* Dropping the primary key constraint on a column that is a serial column throws an error

Not too shabby.

## Get it merged upstream

Once it was all set and nearly all the tests were passing, I prepared to merge it upstream. First, I added JDBC support, and reformatted to match the Sequel style. JDBC was fairly straightforward since the SQL Layer already has a JDBC driver. Most of the challenge was detecting which parts of the adapter were pg specific, and which should be shared, so I'd recommend deciding to do both or one up front if you decide to add a new adapter. After that, I passed it off to Jeremy, who made a few more modifications to support other rubies and rspecs, and a couple more style changes.


## Summary

There were some definite hiccups along the way, but for the most part, adding the new adapter was fairly straightforward, and now you can get the fault tolerance and scalability that the SQL Layer provides with the ease of use and clean code that Sequel provides, so go get started using [FoundationDB's SQL Layer](https://foundationdb.com/layers/sql).


