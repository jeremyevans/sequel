---
 layout: post
 title: Sequel 4.34.0 Released
---

Sequel 4.34.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_34_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A duplicate_columns_handler extension has been added for warning/raising on datasets that return multiple columns with the same name.
* A validators_operator validation has been added to validation_helpers.
* The pg_range extension now supports per-Database custom range types.
* The Dataset#to_hash and related methods now accept an options hash for the object in which to place returned rows.
