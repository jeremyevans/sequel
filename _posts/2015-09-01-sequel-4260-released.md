---
 layout: post
 title: Sequel 4.26.0 Released
---

Sequel 4.26.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_26_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset#grouping_sets has been added for GROUP BY GROUPING SETS support on Postgres 9.5+, MSSQL 2008+, DB2, Oracle, and SQLAnywhere. 
* Sequel::NoMatchingRow exceptions now have a dataset accessor for the dataset that raised the exception.
* The drop_column schema method now supports an :if_exists option for DROP COLUMN IF EXISTS on PostgreSQL.
* Using expressions as values for beginnings and endings of PostgreSQL ranges is now supported in the pg_range extension.
