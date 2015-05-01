---
 layout: post
 title: Sequel 4.22.0 Released
---

Sequel 4.22.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_22_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Sequel no longer busy waits if a connection is not available on ruby 1.9+.
* Sequel now attempts to avoid hash allocations and rehashing, speeding up dataset method chains by almost 20%.
* A csv_serializer plugin has been added, for converting model objects to/from CSV.
* A few less common and untested adapters have been deprecated.
