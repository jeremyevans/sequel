---
 layout: post
 title: Sequel 4.2.0 Released
---

Sequel 4.2.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/4_2_0_txt.html">Full release notes are available</a>, but here are some highlights:

* LATERAL subqueries are now supported via Dataset#lateral.
* A pg_static_cache_updater extension has been added for using PostgreSQL notification channels to update caches used by static_cache plugin.
* A pg_loose_count extension has been added for fast approximate counts of PostgreSQL tables.
* A from_block extension has been added to make it easier to use table returning functions.
* Dataset and Model now both have custom implementations of dup, clone, and freeze.
