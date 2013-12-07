---
 layout: post
 title: Sequel 3.36.0 Released
---

Sequel 3.36.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_36_0_txt.html">Full release notes are available</a>, but here are some highlights:

* An eager_each plugin was added, making Dataset#each on a eager loaded dataset do eager loading.
* The nested_attributes plugin now supports composite primary keys.
* A pg_json extension has been added for dealing with PostgreSQL 9.2's json type.
* A pg_inet extension has been added for dealing with PostgreSQL's inet and cidr types.

