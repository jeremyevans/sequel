---
 layout: post
 title: Sequel 3.38.0 Released
---

Sequel 3.38.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_38_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A pg_row extension has been added to support PostgreSQL row-valued/composite types.
* A pg_row_ops extension has been added for DSL support of PostgreSQL row-valued/composite types.
* A pg_row plugin has been added for representing PostgreSQL composite types as Sequel::Model objects.
* Sequel.expr now splits symbols instead of just wrapping them.
