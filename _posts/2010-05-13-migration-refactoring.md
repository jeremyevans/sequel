---
 layout: post
 title: Migration Refactoring
---

From version 0.1.9.3 to version 3.11.0, <a href="http://github.com/jeremyevans/sequel/blob/3.11.0/lib/sequel/extensions/migration.rb#L133">Sequel's migrator</a> had roughly the same design.  It was a module that was treated like a singleton.  Since it can be used more than once, this requires that it be stateless, which means it used a functional style where all necessary information was passed through method arguments.

While this works fine, it requires more verbose code, and it ended up repeating a lot of operations (such as determining the schema version from the database).  Thankfully, ruby has these things calls objects which can store state, so you can look things up once, store them, and then easily refer to them later without passing them to every function that needs them.

Since I wanted to add a timestamp migrator to Sequel (that lets you apply migrations out of order), I figured it would be best to reuse as much of the existing code as possible.  Unfortunately, modules used as singletons are pretty difficult to reuse.  But ruby has a nice tool that we can use called classes, which can have subclasses, allowing you to have basic behavior in a main class which can be overridden in a subclass.

While it's not too difficult to convert singleton module code to standard class code, it's not completely trivial, especially if you don't have great specs. Sequel did have <a href="http://github.com/jeremyevans/sequel/blob/3.11.0/spec/extensions/migration_spec.rb#L122">decent specs for the migrator</a>.  It even had 100% coverage, just like Sequel's core, Sequel::Model, and all of the built in plugins and other extensions.  Unfortunately, as any good tester will tell you, 100% code coverage means nothing.  I agree, though I also think that less than 100% code coverage means something.

Anyway, Sequel's migrator specs had some issues.  For one, every spec in the migrator spec suite created and tore down two migration directory structures, even though it never modified the migrations themselves.  Second, many of the methods tested were public methods that really should have been private, since it doesn't make any sense for the user to call them.

Since I expect that many programmers are in a similar position, wanting to refactor existing code without having good specs, I'd like to share the technique I used.  It's not amazing in any sense, but I think it has some good general principles:

### Fix the Specs

The first thing you should do if you want to refactor but don't have good specs, is to fix the specs first, before making any changes.  When you are refactoring, you generally want to improve the internals of existing code without modifying the behavior.  To do that, you first need to be sure of what that behavior is, so you can check after refactoring to make sure you didn't change it.

In a series of commits, I started fixing the specs.  I first changed the specs so that they didn't create and tear down directory structures on every spec, and instead <a href="http://github.com/jeremyevans/sequel/commit/624b4ff10a5521a7e2044b5a64d2355c279469d4">used a static directory structure</a> that I committed to the repository.

The next part of the specs to go was the excessive mocking.  Now, all of Sequel's specs except the adapter and integration tests use a mock database, but as much as possible I try to keep the mocking at a fairly low level.  For the migrator case, the migrations that were used called fake database methods that were only defined in the mock database created by the specs.  I modified this so that <a href="http://github.com/jeremyevans/sequel/commit/4110aefdf5f112799730703bb46a7f226f2f7b1b">they called the standard methods you'd call in a migration</a>, create_table and drop_table.  These methods were still mocked out in the specs, but this made the migration files themselves valid.

There were a few minor cleanup changes for the specs, including an important one to <a href="http://github.com/jeremyevans/sequel/commit/95b37d0dce746e0d4a859d02a052c903aef5bf53">remove specs for methods that should be internal</a>, and where possible replace them with specs for the public methods that tested the same thing, but that was pretty much it.

### Make Any Behavioral Changes to Existing Features First

In this case, in addition to refactoring, I was also making some minor behavioral changes as well.  However, I think the best way to be sure that you don't make any unintended changes is to make any behavioral modifications before starting the refactoring.  So I then made the changes mentioned in the <a href="/2010/05/12/migration-improvements.html">previous blog post</a>, checking for duplicate and missing migrations, and saving the migration version after each migration.

There are pros and cons with making behavioral changes first.  An obvious con is that when you refactor, you might want to change the code you just added.  I think a pro is that you get defined behavior before you start the refactoring, which adds additional assurance that the refactoring did not break anything.  In this case, I didn't think the refactoring would require changing much of the recently added code, and the additional assurance was important to me, so I decided to make the behavioral changes first.

After the behavioral changes were made, I added <a href="http://github.com/jeremyevans/sequel/commit/cbeeaa52009bdcb4ed206347c16b49db39bcf9b1">integration tests for the migrator</a>, testing the migrator on a real database.  Sequel didn't have these type of tests before, and I think it's important to test a feature that modifies the database on real databases to ensure that it works.

### Refactor

Next came the <a href="http://github.com/jeremyevans/sequel/commit/d9b4db7e5e6172a68abbaea0b34d7937c0810b2d">major refactoring of the migrator</a>.  If you've never converted a singleton module to a real class, I recommend you check out the link and see the approach.  Basically, in the initialize method, we are setting up all of the state for the migrator.  Then when the methods that actually make changes are called, they just use the stored state instead of having to pass all the state as method arguments.  This greatly simplifies the code.  Since I choose to use attr_reader names that were the same as most of the method argument names, I didn't have to modify that much of the code, and it general it ended up much simpler than before.

Other than removing some public methods that should have been private, the public API wasn't changed at all, because the Sequel::Migrator.run method just instantiated a new instance of the Migrator class and then called the new run instance method.

### Add New Features

Often you are refactoring not for it's own sake, but to add new features that are not possible with the existing implementation.  In this case, the purpose of the refactoring was to add a timestamp migrator.  This type of change cannot be done before the refactoring, which is why I differentiated between behavioral changes to existing features and adding new features.  The former can be done before the refactoring, the latter not till after.

I'll be going over the timestamp migrator in detail in the next blog post, but hopefully this post gives you a good idea how it came to be.
