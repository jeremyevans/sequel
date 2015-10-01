---
 layout: post
 title: Sequel 4.27.0 Released
---

Sequel 4.27.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_27_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A before_after_save plugin has been added, for updating object state before after_save is called instead of after.
* primary_key in create_table blocks now supports a :keep_order option to not make it the first column.
* Dataset#single_record! and #single_value! have been added, which don't require cloning the dataset.
* The pg_json_ops extension now supports the new json/jsonb functions/operators in PostgreSQL 9.5.
