---
 layout: post
 title: Sequel 4.8.0 Released
---

Sequel 4.8.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_8_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A one_through_one association type has been added for a association to a single record through a join table.
* A one_through_many assocation type has been addded to the many_through_many plugin for an association to a single record through multiple join tables.
* An association_join method has been added to model datasets, for setting up joins based on associations.
* eager_graph_with_options has been added to model datasets, with support for eager graphing associations with limits/offsets using a limit strategy.
* A limit strategy is now used in some cases when filtering by associations with limits.
* A limit strategy is now used in some cases when using dataset associations with limits.
