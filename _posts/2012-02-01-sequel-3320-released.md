---
 layout: post
 title: Sequel 3.32.0 Released
---

Sequel 3.32.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_32_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Prepared statements nows support :map/:to_hash types.
* NOT IN handling with an empty array has changed in regards to NULL values.
* You can now define associations that use columns that clash with ruby method names by specifying additional options.
* You can now use models with prepared transactions by disabling after_commit/after_rollback.
* Filtering/excluding by assocations now uses qualified identifiers, so it works even if you do your own joins.
