---
 layout: post
 title: Sequel 3.41.0 Released
---

Sequel 3.41.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_41_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A connection_validator extension has been added to transparently handle disconnected database connections.
* Sequel.delay has been added for generic delayed execution.
* Uniqueness validation now correctly handles nil values.
* Foreign key parsing is now supported on Microsoft SQL Server.
* Sequel now treats clob columns as strings instead of blobs.
