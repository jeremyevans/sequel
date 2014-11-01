---
 layout: post
 title: Sequel 4.16.0 Released
---

Sequel 4.16.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_16_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Model#qualified_pk_hash has been added for getting a hash with qualified primary key columns.
* Database#distinct now accepts virtual row blocks.
* Disconnect errors are now recognized in the postgres adapter when SSL is used.
