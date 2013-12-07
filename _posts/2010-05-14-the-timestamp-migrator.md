---
 layout: post
 title: The Timestamp Migrator
---

As the last couple of blog posts have mentioned, <a href="http://github.com/jeremyevans/sequel/commit/44c90cc62acfd89857a1e7e8dd4208ca06d7b3c9">a timestamp migrator</a> has been added to Sequel.  This blog post is going to explain the purpose of the timestamp migrator, as well as some details.

### Purpose

For about 2 and a half years, Sequel's migrator (now known as the integer migrator) operated like the original ActiveRecord migrator, which used integers starting at 1 for versions.  ActiveRecord added a UTC based migrator in April of 2008, though you could still choose to use the old integer-based migrator by changing the Rails configuration.  So for about 2 years, ActiveRecord has offered more choice than Sequel when it comes to migrators.

For a long time I was resistent to making Sequel's migrator use timestamps as versions.  For single developer projects, they serve no purpose, and for team projects, they trade reliability for convenience.  I've been getting more frequent requests for the feature recently, and came to the realization that for team projects, trading reliability for convenience is a valid choice, and timestamped migrations can add a lot of convenience with a minimal reliability tradeoff if used correctly.

By trading reliability for convenience, I mean that timestamped migrations allow you to apply migrations out of order (the convenience part), with no way to ensure that the migrations due not conflict (the reliability part).  However, assuming your team is judicious in their creation of migrations, never creating simultaneous migrations that conflict, the convenience of being able to apply migrations out of order is a huge bonus over having to renumber integer migrations.  With integer migrations, you'd have to renumber (less convenient), but when you are renumbering, you are more likely to check that the migrations do not conflict (more reliable).

I think part of the reason I took so long to accept this is that I'm pretty much a single developer all the time, so the integer migrator never caused me any pain.

### Features

Just like the integer migrator is modelled on ActiveRecord's old integer migrator, the timestamp migrator is modelled on ActiveRecord's current UTC migrator. Where the integer migrator uses filenames such as:

    001_first_migration.rb
    002_second_migration.rb

The timestamp migrator uses filenames such as:

    20100514163123_first_migration.rb 
    20100514164234_second_migration.rb 

That's if you want to use the same YYYYMMDDHHMMSS timestamp format as ActiveRecord's UTC migrator.  As you might expect from Sequel, the timestamp migrator has some more flexibility.  You can just use dates if you want:

    20100514_first_migration.rb
    20100514_second_migration.rb

Or you can use unix epoch timestamps:

    1273854683_first_migration.rb
    1273855354_second_migration.rb

You can actually use any versioning number system you want, as long as you are consistant and make sure that higher versions should come after lower versions.

As the date example shows, you can have two migrations with the same version, unlike ActiveRecord.  Duplicate migration versions happen rarely by chance, but if you have an automated tool that creates multiple migrations in the same second, they become likely.  Sequel allows duplicate versions as long as the filenames are different (which must be true since they are all stored in the same directory), which should fix the issue.  There is a downside with that, and that is that Sequel doesn't let you change the filenames of applied migrations.  Because Sequel stores the full filename, it would think a renamed migration that was already applied was a new migration.  Sequel handles this process by raising an error if any applied migration is not in the migration directory.

Other than migration file names, the migration files themselves are no different from before.

### Running

You use the timestamp migrator just like you use the integer migrator, using the bin/sequel command line tool with the -m switch:

    sequel -m path/to/migration/directory postgres://host/database

If you want to specify the version to which to migrate, you use the -M switch:

    sequel -m path/to/migration/directory -M 20100514 postgres://host/database

Sequel doesn't have a defined configuration structure where you can set which migrator to use.  It easy to choose which one if you are using the API directly, but there should be a way to handle it via the command line. Since I didn't want to add another flag to bin/sequel (knobs are for knobs), I decided to use a simple heuristic to determine which to use.  If any migration version is over 20000101, bin/sequel will use the timestamp migrator, otherwise it will use the default integer migrator.  I can't imagine anyone will have more than 20000101 migrations, and it's unlikely any timestamp format people would use would have versions under 20000101, so I think it's a valid choice.

### Upgrading

The timestamp migrator handles upgrades from the integer migrator format transparently.  If the table for the integer migrator exists but the table for the timestamp migrator does not, before the timestamp migrator runs the migrations, it will look at the current integer migration version in the database, assume all of those migration files have been applied, and add those migration files to its list of applied migrations.

You should not modify any of your old migrations, so your old migrations will still start at 1.  You'll still be able to migrate all the way up and down even to old migration versions that use the integer numbering scheme.

### Corner Cases

I attempted to work around all of the corner cases I could think of.  One thing that makes the timestamp migrator different from the integer migrator is that you are not necessarily migrating all migrations in the same direction, it's possible that in the same migrator run, you are migrating some up and some down.  This can happen if you are migrating to a specific version that's less than the current version, but have added other migration files that are less than the version to which you are migrating.  In that case, you have applied migrations that are above the version, so you need to migrate them down, but you also have unapplied migrations below the version, and you need to migrate those up.

### Final Thoughts

The timestamp migrator has a full set of specs and integration tests, but it hasn't had a lot of real world testing.  If you want to give it a shot and let me know how it works out, I'd definitely appreciate it.
