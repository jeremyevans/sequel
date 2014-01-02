---
 layout: post
 title: Sequel 4.6.0 Released
---

Sequel 4.6.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/4_6_0_txt.html">Full release notes are available</a>, but here are some highlights:

* The native OFFSET/FETCH support is now used for offsets on Microsoft SQL Server 2012.
* Database#call_mssql_sproc is now supported on MSSQL for calling stored procedures, including handling output parameters.
* Database#commit_prepared_transaction and rollback_prepared_transaction now support sharding via a :server option.
