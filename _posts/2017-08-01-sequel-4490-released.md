---
 layout: post
 title: Sequel 4.49.0 Released
---

Sequel 4.49.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_49_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Database#extend_datasets and Dataset#with_extend now use Dataset::DatasetModule instances for passed blocks.
* Dataset#where_all, #where_each, and #where_single_value have been added as core dataset methods.
* Oracle 12 native limit/offset syntax is now supported, making offsets much faster.
* Many additional features have been deprecated in preparation for the release of Sequel 5 next month.
