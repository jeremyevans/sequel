---
 layout: post
 title: Sequel 4.33.0 Released
---

Sequel 4.33.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_33_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Model.require_valid_table accessor has been added, for not swallowing exceptions when a bad table name is used.
* Database#transaction now supports a :savepoint=>:only option for only creating a savepoint if already inside a transaction.
* On PostgreSQL, Dataset#insert_conflict can now handle an array of columns as the value of the :target option.
