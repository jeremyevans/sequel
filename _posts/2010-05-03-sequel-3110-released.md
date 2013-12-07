---
 layout: post
 title: Sequel 3.11.0 Released
---

Sequel 3.11.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_11_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Sequel now logs the duration of queries.
* You can now log only queries that raise errors or queries that take a long time.
* Sequel now detects when you attempt to save a model object that has been deleted from the database.
* An instance_filters plugin was added, allowing the use of additional filter critera beyond the simple primary key match.
* Database :after_connect and :test options are now supported.

My current main focus is on improving Sequel's documentation, though I'll certainly consider bugs and feature requests submitted from the community.
