---
 layout: post
 title: MyISAM, InnoDB, and PostgreSQL
---

Two weeks ago, <a href="/2010/03/13/dataobjects-versus-native-adapters.html">I looked at differences in performance between the Sequel's DataObjects adapter and the native SQLite, MySQL, and PostgreSQL adapters</a>.  In that posted, I talked about performance differences between MySQL and PostgreSQL, and said that I'd have a follow up post comparing MyISAM, InnoDB, and PostgreSQL performance, which is what this post will do.  Like the last post, this one uses data collected using <a href="http://github.com/jeremyevans/simple_orm_benchmark">simple_orm_benchmark</a>, with the <a href="http://pastie.org/889029.txt">InnoDB results requiring a patch</a>.

First, <a href="http://pastie.org/888600.txt">take a look at the data (in CSV format)</a>.  Here's my summarization of the results:

* InnoDB performs slightly worse than MyISAM when fetching records in most cases, regardless of transaction use.
* PostgreSQL generally performs roughly the same as InnoDB when fetching records.
* InnoDB is a lot slower than MyISAM when not using transactions for inserts, updates, and deletes.
* PostgreSQL is even slower than InnoDB when not using transactions.
* PostgreSQL is generally faster than InnoDB when using transactions.

My conclusion is that, in general, there is no performance reason to choose MySQL over PostgreSQL when using Sequel as the database library.  Replication support now remains the main technical advantage of MySQL over PostgreSQL, and with PostgreSQL 9.0, most of that advantage will be removed.
