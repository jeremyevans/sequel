---
 layout: post
 title: Sequel 3.25.0 Released
---

Sequel 3.25.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_25_0_txt.html">Full release notes are available</a>, but here are some highlights:

* CASCADE support in drop_table, drop_view, drop_column, and drop_constraint
* Datasets can now be used as expressions
* Dataset#select_group has been added for grouping on and selecting the same columns
* Dataset#exclude_where and #exclude_having have been added
* Dataset#select_all now takes arguments and selects argument.*
* Dataset#group and #group_and_count now accept virtual row blocks
