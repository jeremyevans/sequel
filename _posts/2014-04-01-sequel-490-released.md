---
 layout: post
 title: Sequel 4.9.0 Released
---

Sequel 4.9.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_9_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A Dataset::PlaceholderLiteralizer optimization framework has been added.
* Model.first, .first!, .find, .[] are now about 50% faster in some cases.
* The PostgreSQL array parser is up to 1000x faster.
* Dataset#paged_each accepts a :strategy=>:filter option for improved performance.
