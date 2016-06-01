---
 layout: post
 title: Sequel 4.35.0 Released
---

Sequel 4.35.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_35_0_txt.html">Full release notes are available</a>, but here are some highlights:

* The :Bignum symbol is now treated as a generic 64-bit integer type, to work around ruby 2.4's Fixnum/Bignum unification.
* Database#log_connection_info has been added to include connection info in the query log.
* A server_logging extension has been added, which includes server/shard information when logging connection info.
* An sql_comments extension has been added for adding comments to SQL queries, for easier Database server query log analysis.
* Database#skip_locked has been added for skipping locked rows on PostgreSQL 9.5+, Oracle, and Microsoft SQL Server.
