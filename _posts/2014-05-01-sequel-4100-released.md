---
 layout: post
 title: Sequel 4.10.0 Released
---

Sequel 4.10.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_10_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset literalization is up to 3x faster.
* Regular association loading is up to 85% faster.
* Eager loading limited associations now uses a UNION-based strategy by default.
* Dataset#import and #multi_insert now insert multiple rows in a single query on most databases.
* Fetching records is about 20% faster in the jdbc adapter.
* Database#transaction now has an :auto_savepoint option for automatically creating savepoints in nested transactions.
* Common table expressions are now supported on SQLite 3.8.3+.
