---
 layout: post
 title: Sequel 4.29.0 Released
---

Sequel 4.29.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_29_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A uuid plugin has been added, which will automatically create a UUID for new model objects.
* Model#json_serializer_opts has been added to the json_serializer plugin, allowing for overriding JSON serialization options at an instance level.
* The Database#transaction :retry_on option now works when using savepoints.
* Calling Database#table_exists? in a transaction will no longer abort the transaction in some databases.
* Blobs can now be used as bound variables in the oracle adapter.
