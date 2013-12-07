---
 layout: post
 title: Sequel 3.28.0 Released
---

Sequel 3.28.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_28_0_txt.html">Full release notes are available</a>, but here are some highlights:

* DB2 support has been improved greatly.
* Firebird support has been improved significantly.
* Eager loading of associations with :limit now works correctly.
* Dataset#map, #to_hash, and related methods can now take arrays of symbols for arrays of results.
* RETURNING is now supported in UPDATE/DELETE statements on PostgreSQL 9.1.
* WITH is now supported in INSERT/UPDATE/DELETE statements on PostgreSQL 9.1.
