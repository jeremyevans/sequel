---
 layout: post
 title: Helping Other Projects
---

Last week I posted about how I test Sequel.  While Sequel's test suites definitely protect Sequel against regressions, they also have the ability to find bugs in ruby implementations, database adapters, and databases themselves.  Sequel's test suites have uncovered the following bugs:

* <a href="http://svn.ruby-lang.org/cgi-bin/viewvc.cgi?view=rev&revision=22679">A singleton class issue in MRI 1.8.7</a>, now fixed.
* <a href="http://redmine.ruby-lang.org/issues/show/3268">A timezone issue on Windows in MRI 1.9.1</a>, still open.
* <a href="http://jira.codehaus.org/browse/JRUBY-4792">A --1.9 BasicObject issue in JRuby</a>, still open.
* <a href="http://github.com/tenderlove/sqlite3-ruby/commits/dda2f3b8ab8c8890de4de66447ff7b88044d4740">Bugs in sqlite3-ruby-1.3.0-beta.1</a>, now fixed.
* <a href="http://www.sqlite.org/src/tktview/3338b3fa19ac4abee6c475126a2e6d9d61f26ab1">A bug in SQLite's JOIN USING implementation</a>, still open.
