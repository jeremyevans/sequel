---
 layout: post
 title: Sequel 4.48.0 Released
---

Sequel 4.48.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_48_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Many additional features have been deprecated in preparation for the release of Sequel 5.
* The Model#to_json and Dataset#to_json methods in the json_serializer plugin now accept a block for customizing output.
* Dataset#as_hash has been added, allowing for the possible removal of #to_hash in cases where the definition of #to_hash causes issues.
