---
 layout: post
 title: Writing The FoundationDB SQL Layer Adapter
---


I work at [FoundationDB](https://foundationdb.com/) on the team for the [SQL Layer](https://foundationdb.com/layers/sql), and one of my tasks this summer was to develop an adapter for Sequel to work with the SQL Layer. Since the SQL Layer implements ANSI SQL, this was fairly straightforward, although there were some hiccups. The new adapter was released with version 4.15.0 of Sequel at the beginning of October. Here I will chronicle my experience writing a new adapter for Sequel. 

My basic approach was simple:

1. Inject a stump SQL Layer adapter
2. Get tests to run against the new adapter
3. Get as many tests as possible passing without actually touching the SQL layer, filing bugs along the way
5. Go through all those `supports_*` flags to get as many tests as possible actually running
6. Fix said bugs, or find out that they won't be fixed any time soon
7. Make patched version of Sequel that skips the failing tests
8. Update to support 4.13.0
9. Get it merged upstream


## Injection
Since the SQL Layer ended up being merged in, this step ended up being unnecessary, but if you want to add a less standard or less complete SQL implementation that doesn't fit with actually being merged into Sequel proper, this might be a necessary approach.

It seemed like it could be tricky, but I poked around the internet and found [Sequel Vertica](https://github.com/camilo/sequel-vertica), another external adapter for Sequel. This made the injection pretty easy, thanks in part to Sequel. Just push the new adapter protocol onto `Sequel::Database::Adapters` and define the `Sequel::Fdbsql::Database` class and put it at the relative path of `sequel/adapters/fdbsql.rb`. Now, ignoring the fact that every function in Sequel falls over with the new adapter, it works great.

## Running Tests
Fortunately [SQL Layer adapter for ActiveRecord](https://github.com/FoundationDB/sql-layer-adapter-activerecord), runs the ActiveRecord tests using our adapter and the underlying SQL Layer. Thanks to ruby's power you can get the path to any gem after requiring it:

    require 'sequel'
    SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path

Awesome, now I can plug the relative test path for the interaction tests into spec and run the integration tests. Sequel already did it's part by allowing you to specify an environment variable that specifies the connection URL for any given adapter. So I set `ENV[SEQUEL_INTEGRATION_URL]` to my localhost: `'fdbsql://user@localhost:15432/sequel_testing'` and I'm off and running.

Well, what happened? Every test failed, obviously. But they all used my adapter, and local instance of the SQL Layer.

## Get as many tests passing as possible without touching the SQL Layer

This was the fun part, writing a whole bunch of ruby code. Since I had done barely any SQL for a few years, it also meant lots of remembering SQL syntax and how a join works and what not.
Of course the test failures started out being exceptions in the before and after blocks, and every test was failing because I hadn't implemented `DROP TABLE` (or technically `DROP TABLE IF EXISTS`) and `CREATE TABLE`. But, in order to do that I needed a real connection, so, I installed [`ruby-pg`](https://bitbucket.org/ged/ruby-pg/wiki/Home). Once I got it installed, getting it to work with the SQL Layer and Sequel was a non-issue, probably helped by the fact that our ActiveRecord adapter uses the `pg` gem, and nearly all the adapters use the Postgres Protocol. Don't be fooled though, the SQL Layer may speak the Postgres protocol, but it has lots of differences from Postgres. I'll point out some of the differences that surprised me. For the most part though, a little bit of SQL knowledge, and referencing the way the Postgres adapter was written got me through issues.

### SQL Functions

This didn't hit a ton of tests, but after the simple things, like `CREATE`, `DROP`, etc. were implemented, I had to start somewhere, and parser errors seemed like as good a place as any. One of the first ones that I hit  was, something like: `PG::SyntaxErrorOrAccessRuleViolation: ERROR:  Encountered " "<<" "<< "" at line 1, column 13.`. Of course my first reaction was that the SQL Layer didn't have bitwise operators, but, it turns out that it does have the operations, but they're functions. The same goes for regular expressions. So instead of `a !~ b` it becomes `REGEX(a,b)`, which conveniently just required me to override `complex_expression_sql_append`, and for a set of operators use the functions instead. (The `*_append` is a pretty common point of customization in Sequel). On a similar front, to escape binary data in the SQL Layer, you use a preceding x: `x'a390fb33d2b'`. But the errors that come back aren't easy to distinguish between X isn't supported and use a different syntax. Unfortunately, providing a helpful error message would be just as much work as actually implementing the handling for those binary operators.

### Schema parsing

First I took the ActiveRecord implementation as a reference, because the SQL Layer is quite different from Postgres. I couldn't figure out how to query, through Sequel's nice syntax, a schema that wasn't the current schema though. So I just took the SQL text that ActiveRecord sent along, and made a few tweaks to align with what Sequel wanted to know. After doing that, and doing similar things for tables, and views, and indexes, and primary keys, I discovered Sequel's shortcut for schemas and aliases: `scheme__column_name___alias`. This meant that I could rewrite the whole thing in ruby without a [big string](https://github.com/FoundationDB/sql-layer-adapter-sequel/blob/6e7af92f6d8b22ee37a1cc8e667af8bccc697d14/lib/sequel/adapters/fdbsql.rb#L221) in the middle of my file. I did continue to use the `Sequel.as` method even after learning about the symbol shortcut, just because it was more readable, either way though, much better than a block of SQL code. 

### Deal with retry stuff

So, one thing that differs about FoundationDB's transactions versus a lot of other popular SQL instances, is that occasionally, you get a variety of retryable errors, such as `past_version` (if the system is under too much load you may get this). As with the Sequel convention, these get bubbled up, but it is something that application developers will have to be aware of when using Sequel (or any other adapter) with the FoundationDB SQL Layer.

## Add Supports flags

Sequel has a whole bunch of `supports_*` methods that determine how Sequel will interact with the adapter, and which tests to run. The main challenge here was figuring out whether it was something we didn't support, or if it was just something where we had a different syntax (like the bitwise operators). One ended up being rather tricky, `uses_with_rollup?`, because when enabled the SQL Layer was just ignoring the `WITH ROLLUP` part of the statement, and giving incorrect results (this has been changed to throw an error, until it's supported). So, I guess the moral here, is that if you're going to implement an adapter it's a good idea to know what the database actually supports.

## Fix bugs

Now that I had the 611 tests running, Sequel started to expose a couple bugs in the SQL Layer. There were a few, which is impressive considering all the other adapters' tests that we run, and all of our own tests that we have. Some of these were slightly embarrassing: `select 1 where true is not true` was returning a single row with `1`, even though `select true is not true` correctly returned `false`. Others were more complicated: `SELECT * FROM "artists" WHERE (("artists"."id" NOT IN (SELECT "albums_artists"."artist_id" FROM "albums_artists" INNER JOIN "albums" ON ("albums"."id" = "albums_artists"."album_id") INNER JOIN "albums_artists" AS "albums_artists_0" ON ("albums_artists_0"."album_id" = "albums"."id") WHERE (("albums_artists_0"."artist_id" IN (1, 4)) AND ("albums_artists"."artist_id" IS NOT NULL)))) OR ("artists"."id" IS NULL))`, which was incorrectly returning 512 rows on a table that had 4 rows. These kind of complicated `IN ... NOT IN ...` queries are mostly used by the model associations in Sequel, but now those crazy queries work.
There were also some things that just inspired me to improve some slight usability issues, such as having an implicit `NOT NULL` when specifying a column as a `PRIMARY KEY`

## Update to 4.13.0

Well, it turns out the Sequel repository is pretty active, so between starting and finishing, a new version came out: 4.13.0. With this new version of Sequel, I had to add a new method to the adapter, for a few failing tests, but then, it was all set for 4.13.0.

## Patch Sequel tests

When all was said and done, and all the bugs were fixed, it left a few things that just aren't handled:

* The SQL Layer doesn't support temporary tables yet
* It doesn't support Constraint Checks
* Dropping the primary key constraint on a column that is a serial column throws an error

Once this was completed I opened a pull request to see if this could be merged upstream. None of the adapters have been merged upstream, so I wasn't too optimistic. Jeremy Evans replied with the very defensible position of not having hard coded checks in the tests for an adapter that was not part of Sequel. However since there were such a small portion of tests that the SQL Layer wasn't passing, he suggested merging it upstream.

## Get it merged upstream

With this I added JDBC support, and reformatted to match the Sequel style. JDBC was fairly straightforward since the SQL Layer already has a JDBC driver, once I got jruby to recognize the jar (setting the classpath to the path to the jar: `CLASSPATH=/path/to/fdb-sql-layer-jdbc-2.0-0-jdbc41.jar`) it was not a lot of code to get it to work. Most of the challenge was detecting which parts of the adapter were pg specific, and which should be shared. After that, I passed it off to Jeremy, who made a few more modifications to support other rubies and rspecs, in addition to a few more things to make it conform to the Sequel style.


## Summary

There were some definite hiccups along the way, but for the most part, adding the new adapter was fairly straightforward, and now you can go try out Sequel with the SQL Layer.


