---
 layout: post
 title: Sequel 4.7.0 Released
---

Sequel 4.7.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_7_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Alternatives have been added for the more complex virtual row block methods.
* An update_or_create plugin has been added, for updating a record if it exists, or creating a new one if it does not.
* Sequel now automatically rolls back transactions in killed threads on ruby 2.0+.
