---
 layout: post
 title: Sequel 3.42.0 Released
---

Sequel 3.42.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_42_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset #sum, #avg, #min, #max, #range, and #interval methods now accept virtual row blocks.
* Database#do has been added on PostgreSQL for anonymous procedural language function execution.
* Database#copy_table and #copy_into are now supported on jdbc/postgres.
* Sequel now supports deferred constraints on PostgreSQL and Oracle.
