---
 layout: post
 title: Sequel 4.43.0 Released
---

Sequel 4.43.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_43_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Database#freeze has been implemented, and it is recommended to use it in production and during testing.
* Model#refresh now uses the same optimization that Model.with_pk uses.
* The prepared_statements plugin no longer uses prepared statements in cases where it is likely to be slower.
* Multiple thread safety issues in adapters found during implementation of Database#freeze have been fixed.
