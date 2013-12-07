---
 layout: post
 title: Migration Improvements
---

Sequel's migrator has been pretty much unchanged since it was <a href="http://github.com/jeremyevans/sequel/commit/a9f89a012d313923f9b6c1b00e20a07842054658">originally committed in August 2007</a>.  There have been a few bug fixes here and there, but assuming you write your migrations well, it works fine.

Unfortunately, not everyone writes their migrations well.  I know I've made a made a mistake or two when writing migrations.  While Sequel's migrator worked fine in the normal case, it didn't correctly handle mistakes in migrations or missing or duplicate migration files.

### Mistakes in Migrations

For example, let's say you have 3 migrations that you are working on, and the middle one raises an exception.  The way Sequel's migrator worked, it did not update the migration version until after applying all migrations.  So it would apply the first migration successfully, attempt to apply the second migration, which raised an error, and then stop.  The database would show that the version hadn't been modified, even though the first migration was ran successfully.

Now Sequel did run all migrations inside the same transaction, so for databases that support transactional schema modifications, such as PostgreSQL, this wasn't really a problem.  However, it didn't work well on databases such as MySQL that don't support transactional schema modifications, since if you tried to migrate later (after fixing the problem with the second migration), it would try to run the first migration again, even though it had already been run.

One of the recent migration changes was to <a href="http://github.com/jeremyevans/sequel/commit/12d1dc7ba46cf89d3878e6e65d00dd8563a5691a">run each migration in a separate transaction and update the migration version after each successful migration</a>.  This should fix the problem with the same migration being run more than once.

### Duplicate Migrations

Let's say you are working in a team, and two members of the team create a new migration, giving it the same version number (1 more than the last committed migration), but different names.  When the changes are merged, the source control program won't detect any conflicts, since the files have the same names.  However, when you ran Sequel's migrator, it would choose one of the two files, and ignore the other one.  It didn't do this on purpose, just as a side effect of the way it was programmed.

This can obviously lead to problems, as the behavior appeared nondeterministic (it would pick the last entry in the directory, and directory order isn't obvious).  With a recent commit, Sequel will now <a href="http://github.com/jeremyevans/sequel/commit/e6aa4451541494ebfbaa493d52e25a0a5f6d9c41">raise an error if it detects duplicate migration versions</a>.

### Missing Migrations

Let's say you are working in the same team.  You've learned from the previous mistake about duplicate migrations, and having good team communication, you know that someone else has started work on migration version 25 (with the current production migration version being 24).  You need to work on your own migration, but not wanting to step on his toes, you give your migration version 26.  His migration version hasn't been committed yet, but since you need to get some work done, you decide to run the migrator on your development database.

When you migrate your development database to the latest version, Sequel's migrator wouldn't complain about a missing version 25, and would happily migrate the version to 26.  Later, when your coworker commits his changes and you now have version 25 in your repository, you try to run the migrator again.  This time, the migrator does nothing, since the version is at 26, so it thinks version 25 has already been committed.

Because missing migrations usually lead to problems later, <a href="http://github.com/jeremyevans/sequel/commit/e6aa4451541494ebfbaa493d52e25a0a5f6d9c41">Sequel's migrator now raises an exception for missing migration versions</a>, which saves you from getting into this mess in the first place.  So what do you do if you want to work in a team where multiple people are creating migrations, and you want to be able to run migrations out of order?  Sequel now has a solution for you, the TimestampMigrator, which will be discussed in a later blog post.
