---
 layout: post
 title: Sequel 4.23.0 Released
---

Sequel 4.23.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_23_0_txt.html">Full release notes are available</a>, but here are some highlights:

* An update_refresh plugin has been added for refreshing model instances when updating.
* A delay_add_association plugin has been added for delaying add_* association methods until after updating.
* Database#transaction now returns the block return value if :rollback=>:always is used.
* Sequel's specs have been converted from rspec to minitest/spec, and many test order dependency bugs have been fixed.
