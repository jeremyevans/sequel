---
 layout: post
 title: Sequel 4.14.0 Released
---

Sequel 4.14.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_14_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Delayed evaluation blocks can now take the dataset doing the literalization as an argument.
* You can now pass arbitrary types that can be correctly literalized to Dataset#where and similar methods.
* association_join now works correctly if the dataset already has an explicit selection.
* Code examples in the RDoc are now syntax highlighted.
