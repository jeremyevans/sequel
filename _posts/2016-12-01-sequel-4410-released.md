---
 layout: post
 title: Sequel 4.41.0 Released
---

Sequel 4.41.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_41_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset#with_row_proc and similar methods have been added for returning clones with settings changed.
* The :offset_strategy Database option is now supported on DB2, for using native instead of emulated offsets.
* The association dataset methods now handle nil keys correctly.
* The ado adapter has been greatly improved.
