---
 layout: post
 title: Sequel 4.30.0 Released
---

Sequel 4.30.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_30_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Overriding the :limit and :eager_limit_strategy association options can now be done on a per-call basis.
* Dataset#insert_conflict and #insert_ignore have been added on SQLite.
* An identifier_columns plugin has been added, which allows Model#save to work when column names contain double underscores.
* IPv6 addresses are now supported in connection URLs.
