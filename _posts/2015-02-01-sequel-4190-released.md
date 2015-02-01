---
 layout: post
 title: Sequel 4.19.0 Released
---

Sequel 4.19.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_19_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Model#get_column_value and #set_column_value have been added for getting/setting column values.
* A column_conflicts plugin has been added to automatically handle column names that conflict with existing method names.
* An accessed_column plugin has been added which records which columns have been accessed for an instance.
* Model#cancel_action has been added for canceling the action inside before hooks.
