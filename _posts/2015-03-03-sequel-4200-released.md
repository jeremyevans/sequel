---
 layout: post
 title: Sequel 4.20.0 Released
---

Sequel 4.20.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_20_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A :before_retry option has been added to Database#transaction, for a proc to call when before retrying.
* You can now specify the root object key for JSON hashes in the json_serializer plugin by using a String value for the :root option.
* The parent association is now set when loading descendants in the rcte_tree plugin.
* Eager loading associations with limits and eager blocks now work in more cases.
