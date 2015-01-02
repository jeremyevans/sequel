---
 layout: post
 title: Sequel 4.18.0 Released
---

Sequel 4.18.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_18_0_txt.html">Full release notes are available</a>, but here are some highlights:

* An :auto_increment key has been added the schema hashes for primary key columns.
* Dataset#empty? now ignores an existing order on the dataset.
* PG::ConnectionBad exceptions are now raised as disconnect errors in the postgres adapters.
