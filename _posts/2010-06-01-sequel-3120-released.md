---
 layout: post
 title: Sequel 3.12.0 Released
---

Sequel 3.12.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_12_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A timestamp migrator was added, allowing more flexibility for migrations when working in teams.
* A new migration DSL was added, simplifying the migration file format.
* A model sharding plugin was added, which lets model objects work well with Sequel's sharding support.
* Substantial improvements were made to Sequel's documentation, mainly in the form of new guides.

One of my foci for 3.13.0 is on adding some more model plugins, specifically ones for to_xml and to_json. As always, I'll certainly fix bugs and consider feature requests submitted from the community.
