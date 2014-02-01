---
 layout: post
 title: Sequel 3.44.0 Released
---

Sequel 3.44.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_44_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset#paged_each has been added for processing large datasets without keeping all rows in memory.
* Constraint violations in the database are now raised as specific Sequel::ConstraintViolation subclass instances.
* Performance has been increased in a number of different areas.
* The columns_introspection extension can now introspect more cases.
