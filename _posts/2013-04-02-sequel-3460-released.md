---
 layout: post
 title: Sequel 3.46.0 Released
---

Sequel 3.46.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_46_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Added Dataset#first!, like #first but raises a NoMatchingRow exception instead of returning nil.
* Added Dataset#with_pk! for model datasets, similar to #first!.
* Added drop_foreign_key to the alter table generator, for easily dropping foreign key constraints.
* Added Support for Microsoft SQL Server CROSS/OUTER APPLY.
* Sped up threaded connection pools when :connection_handling=>:queue is used.

In other news, Sequel 4 implementation planning has started, <a href="https://github.com/jeremyevans/sequel-4-plans">please review and provide feedback</a>.
