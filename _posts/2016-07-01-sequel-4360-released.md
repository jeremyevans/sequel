---
 layout: post
 title: Sequel 4.36.0 Released
---

Sequel 4.36.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_36_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Using namespaced models is now easier due to the addition of Model and def_Model class methods to Sequel::Model.
* A string_agg extension has been added for aggregate string concatenation on most supported databases.
* A connection_expiration extension has been added for automatically removing connections from the pool.
* Support for <, <=, >, and >= operator validations with integer and string types has been added to constraint_validations.
* Using the Bignum class as a generic 64-bit integer type is now deprecated.
