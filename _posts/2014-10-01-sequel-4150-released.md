---
 layout: post
 title: Sequel 4.15.0 Released
---

Sequel 4.15.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_15_0_txt.html">Full release notes are available</a>, but here are some highlights:

* fdbsql and jdbc/fdbsql adapters have been added, for connecting to FoundationDB SQL Layer.
* A split_values plugin has been added for splitting column values from non-column values for model instances.
* A Sequel::Model.cache_associations accessor has been added, useful when using development mode code reloading.
* More PostgreSQL array types are automatically handled by the pg_array extension.
