---
 layout: post
 title: Sequel 4.31.0 Released
---

Sequel 4.31.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_31_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Sequel now works with ruby 2.3's --enable-frozen-string-literal.
* Migrators now raise an exception for migration files that don't contain a single migration.
* The jdbc/postgresql adapter now supports PostgreSQL-specific types in bound variables.
* The jdbc/postgresql adapter now works with JRuby 9.0.5.0.
