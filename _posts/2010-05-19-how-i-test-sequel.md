---
 layout: post
 title: How I Test Sequel
---

Sequel is well known for sticking with a time-based and not feature-based release cycle (with a release almost every month).  It's also known for having a no-regressions policy, where no code is pushed to the <a href="http://github.com/jeremyevans/sequel">master branch on GitHub</a> unless it passes all of the specs.  That doesn't mean that there aren't commits that break specs, just that the master branch on GitHub isn't updated until later commits fix the issues.

This post is going to explain how I test Sequel to ensure a consistently high level of quality.  To use a phrase I overuse in presentations, it's actually quite simple.

First, Sequel does not use a CI (Continuous Integration) testing tool.  In general, a CI tool can only test code after it has been committed, which opens the code up to regressions.  I could potentially use a CI tool that worked off a private repository, but I haven't felt the need.

Instead, I take a simple approach.  Test first, then commit.  You might ask, don't all decent programmers do that?  Surely only a buffoon would check in code without running the tests, right?

Well, in the case of Sequel, the testing is a bit more involved then you might think.  Before pushing any code to Github, I run it through many test suites:

### OpenBSD Test Suites

I develop Sequel on OpenBSD, and that is the primary testing platform.

* Main specs on MRI-1.8.7p248 (1.8), MRI-1.9.1p243 (1.9), and JRuby-1.3.1 (jruby) (3 test suites)
* Plugin specs on 1.8, 1.9, and jruby (3 test suites)
* Native sqlite adapter and integration tests on 1.8 and 1.9 (2 test suites)
* Amalgalite adapter and integration tests on 1.8 and 1.9 (2 test suites)
* Native postgres adapter and integration tests on 1.8 and 1.9 using pg and jruby using jeremyevans-postgres-pr (3 test suites)
* Native mysql adapter and integration tests on 1.8 and 1.9 (2 test suites)
* DO (DataObjects) adapter and integration tests on 1.8 and 1.9 on MySQL, PostgreSQL, and SQLite (6 test suites)
* JDBC adapter and integration tests on jruby on MySQL, PostgreSQL, SQLite, and H2 (4 test suites)

### Windows Test Suites

I started running test suites on Windows in July 2009 in order to increase support for Microsoft SQL Server.

* Main specs on MRI-1.8.6p0 (1.8), MRI-1.9.1p378 (1.9), and JRuby-1.5.0 (jruby) (3 test suites)
* Plugin specs on 1.8, 1.9, and jruby (3 test suites)
* ODBC adapter and integration tests on 1.8 on MSSQL (1 test suite)
* ADO adapter and integration tests on 1.8 and 1.9 on MSSQL using both standard and SQLNCLI10 providers (4 test suites)
* JDBC adapter and integration tests on jruby on MSSQL using JTDS and SQLServer (2 test suites)

I was testing in --1.9 mode with JRuby 1.4.0, but I've been talking to the JRuby developers and it looks like it's not reporting things correctly, so I've stopped those tests until JRuby better supports --1.9 (<a href="http://jira.codehaus.org/browse/JRUBY-4792">it still has some issues with BasicObject</a>).

### Personal Test Suites

Usually right before pushing to Github, I'll run things through my personal test suites.

* 3 personal applications unit and integration tests (6 test suites)
* sequel_postgresql_triggers specs (1 test suite)
* sequel_validation_helpers_block specs (1 test suite)
* giftsmas unit and integration tests (2 test suites)
* scaffolding extensions tests (1 test suite)

Combined that's almost 50 separate test suites, and they are all run before every GitHub push to try to avoid all known regressions.  

### Release Testing

The only additional testing I usually do before releases is running the unit and integration test suites for 4 more applications that use the gem version of Sequel instead of the git checkout.  I test those with the new gem before pushing the gem to rubygems.org, just to make sure there aren't any regressions.
