---
 layout: post
 title: Sequel 3.14.0 Released
---

Sequel 3.14.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_14_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Dataset#grep now supports :all_patterns, :all_columns, and :case_insensitive options.
* Model#save can now take a :raise_on_failure option to override the raise_on_save_failure setting.
* The tree plugin now accepts a :single_root option for ensuring only a single tree root.
* An important bug was fixed in the timezone support.

Not much is currently planned for 3.15.0. As always, I'll certainly fix bugs and consider feature requests submitted from the community.  I'm happy to announce that 3.14.0 is the first release where the majority of new features and bug fixes were submitted by the community.
