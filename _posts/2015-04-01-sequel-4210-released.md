---
 layout: post
 title: Sequel 4.21.0 Released
---

Sequel 4.21.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_21_0_txt.html">Full release notes are available</a>, but here are some highlights:

* SQL::GenericExpression#=~ has been added for easier equality/inclusion/identity expressions in virtual rows.
* SQL::GenericExpression#!~ has been added on ruby 1.9 for easier inverted equality/inclusion/identity expressions in virtual rows.
* Database#add_named_conversion_proc has been added on PostgreSQL for adding conversion procs for named types.
* Database#transaction now works correctly inside after_commit/after_rollback hooks.
