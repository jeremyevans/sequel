---
 layout: post
 title: Sequel 4.42.0 Released
---

Sequel 4.42.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_42_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Frozen datasets now work in almost all cases.
* Frozen datasets are actually frozen on ruby 2.4, and frozen dataset opts are frozen on ruby <2.4.
* Frozen datasets can actually be up to 3x faster now than unfrozen datasets due to caching.
* A freeze_datasets Database extension has been added to freeze a Database instances datasets by default.
* Many dataset methods are now available in model dataset_module blocks for defining dataset methods, with caching for frozen datasets.
