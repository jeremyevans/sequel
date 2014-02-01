---
 layout: post
 title: Sequel 3.15.0 Released
---

Sequel 3.15.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_15_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A mysql2 adapter was added, which offers a large performance improvement over the mysql adapter.
* Support for sequel_pg was added to the postgres adapter when pg is being used, for a large performance improvement.
* Mass assignment has been made about 10x faster.
* Sequel now handles models with aliased table names better in associations.

I have some plans already for 3.16.0, one of which is a <a href="http://github.com/shanna/swift">swift</a> adapter.  As always, I'll certainly fix bugs and consider feature requests submitted from the community.
