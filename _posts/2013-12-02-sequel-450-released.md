---
 layout: post
 title: Sequel 4.5.0 Released
---

Sequel 4.5.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_5_0_txt.html">Full release notes are available</a>, but here are some highlights:

* An mssql_optimistic_locking plugin was added, for using a timestamp/rowversion column for optimistic locking.
* Unique constraints are now copied when emulating alter table on SQLite.
* On DB2, use_clob_as_blob now defaults to false.
