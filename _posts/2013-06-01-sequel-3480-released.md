---
 layout: post
 title: Sequel 3.48.0 Released
---

Sequel 3.48.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_48_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Features to be removed in Sequel 4 now have deprecation warnings.
* Dataset creation and cloning are faster.
* Model.with_pk is now faster.
* The pg_hstore_ops extension now integrates better with other Sequel pg_* extensions.
* Sequel.object_to_json was added, allowing to to easily use alternative JSON libraries with Sequel.
* The association_proxies plugin now gives the user control over where the methods are sent.
* Offset support is now emulated in Microsoft Access.

This is the last minor release in the Sequel 3 series.  Sequel 4 will be released next month, with the deprecated behavior removed.
