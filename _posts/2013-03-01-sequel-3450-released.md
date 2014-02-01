---
 layout: post
 title: Sequel 3.45.0 Released
---

Sequel 3.45.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_45_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Database#transaction now supports a :retry_on option for automatically retrying transaction blocks.
* The json_serializer and xml_serializer plugins are now secure by default.
* Serialization failures are now raised as Sequel::SerializationFailure exceptions.
* Transaction isolation levels are now supported on more databases.
* Metadata parsing on PostgreSQL now correctly handles tables with the same name in multiple schemas.
