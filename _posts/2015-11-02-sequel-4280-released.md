---
 layout: post
 title: Sequel 4.28.0 Released
---

Sequel 4.28.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_28_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A subset_conditions plugin was added to make it easier to get the filter conditions used for a subset.
* A boolean_subsets plugin was added for automatic creation of subsets for boolean columns.
* Model#refresh now raises Sequel::NoExistingObject if the object no longer exists.
* The list plugin now works correctly when there is a validation on the position column.
