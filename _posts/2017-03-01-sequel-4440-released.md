---
 layout: post
 title: Sequel 4.44.0 Released
---

Sequel 4.44.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_44_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Model.freeze has been implemented, and it is recommended to use it in production and during testing.
* Model.finalize_associations has been added, speeding up association methods.
* Model.freeze_descendents has been added to the subclasses plugin, useful for freezing all model classes in use.
* An implicit_subquery extension has been added that makes most dataset query methods operate correctly on datasets using raw SQL.
* Model datasets now support optimized where_each, where_all, and where_single_value methods.
