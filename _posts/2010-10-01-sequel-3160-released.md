---
 layout: post
 title: Sequel 3.16.0 Released
---

Sequel 3.16.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_16_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A swift adapter was added, offering an improvement in SELECT performance over the postgres adapter.
* DateTime literalization on ruby 1.9 has been fixed.
* The rcte_tree and lazy_attributes plugins can now be used together.
* The identity_map plugin handles composite keys for many_to_one associations.

I currently don't have any major plans for 3.17.0.  As always, I'll certainly fix bugs and consider feature requests submitted from the community.
