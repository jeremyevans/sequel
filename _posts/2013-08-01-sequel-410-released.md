---
 layout: post
 title: Sequel 4.1.0 Released
---

Sequel 4.1.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/4_1_0_txt.html">Full release notes are available</a>, but here are some highlights:

* mysql2 streaming is now supported via Dataset#stream.
* Database#run and #<< accept placeholder literal strings.
* You can now provide options when creating check constraints.
* The prepared_statements plugin no longer breaks instance_filters or update_primary_key.
