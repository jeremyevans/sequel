---
 layout: post
 title: Sequel 3.37.0 Released
---

Sequel 3.37.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_37_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Database#extension and Dataset#extension were added for easier extension usage.
* Dataset join methods now respect a :qualify=>:deep option for automatic deep qualification of conditions.
* All of Sequel's model associations can now use arbitrary expressions as keys/join conditions.
* The pg_array extension has been made much more generic, and supports a wider number of array types.
* A pg_range extension has been added for dealing with PostgreSQL 9.2+ range types.
* A pg_interval extension has been added for returning intervals as ActiveSupport::Duration instances.
