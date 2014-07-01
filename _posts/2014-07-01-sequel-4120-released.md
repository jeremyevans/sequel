---
 layout: post
 title: Sequel 4.12.0 Released
---

Sequel 4.12.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_12_0_txt.html">Full release notes are available</a>, but here are some highlights:

* The auto_validations plugin now sets up automatic max_length validations for string columns.
* Model#set_nested_attributes has been added to the nested_attributes plugin, for per-call options.
* Database#values has been added on PostgreSQL to use a VALUES query.
* The sqlite adapter now supports a :readonly option.
