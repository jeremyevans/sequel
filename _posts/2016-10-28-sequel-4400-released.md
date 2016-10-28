---
 layout: post
 title: Sequel 4.40.0 Released
---

Sequel 4.40.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_40_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Sequel.split_symbols setting has been added, which allows the disabling of symbol splitting.
* Qualified identifiers can now be created via `Sequel[:table][:column]`
* symbol_aref and symbol_aref_refinement extensions have been added for creating qualified identifiers via `:table[:column]`
* symbol_as and symbol_s_refinement extensions have been added for creating aliased identifiers via `:column.as(:alias)`
* An s extension has been added allows easier calling of Sequel.expr via `S(obj)`
