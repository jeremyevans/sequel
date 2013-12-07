---
 layout: post
 title: Sequel 3.24.0 Released
---

Sequel 3.24.0 was released today, and it's the largest release in terms of features in over a year!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_24_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A prepared_statements plugin was added for using prepared statements for creating, updating, deleting, and looking up models by primary key.
* A prepared_statements_safe plugin was added for extra safety against denial of service attacks when using the prepared statement plugin.
* A prepared_statements_association plugin was added for using prepared statements for regular association loading.
* Dataset#with_pk was added to model datasets for looking up the record matching the given primary key value.
* A prepared_statements_with_pk plugin was added for using prepared statements for Dataset#with_pk.
* You can now exclude by associations, filter/exclude by multiple associated objects, and filter/exclude by association datasets.
* Sequel now supports around_hooks for all of its before/after hook types.
* Dataset#[] for model datasets with a single integer argument will call Dataset#with_pk.
* A defaults_setter plugin was added for automatically setting database default values on new model instances.
* Database#views has been added to get an array of view names.
* Database#create_table? now uses CREATE TABLE IF NOT EXISTS if such syntax is supported.
