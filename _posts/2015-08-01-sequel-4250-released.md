---
 layout: post
 title: Sequel 4.25.0 Released
---

Sequel 4.25.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_25_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset#group_append has been added for appending to an existing GROUP BY clause.
* An inverted_subsets plugin has been added for automatically creating inverted subsets.
* A singular_table_names plugin has been added for making singular table names the default.
* Dataset#insert_conflict has been added on PostgreSQL 9.5+, supporting the ON CONFLICT clause of insert, allowing upsert functionality.
* You can now use Dataset#returning when using prepared statements.
