---
 layout: post
 title: Sequel 3.21.0 Released
---

Sequel 3.21.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_21_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A tinytds adapter was added that makes it easy to connect to Microsoft SQL Server from unix.
* An association_autoreloading plugin was added that removes stale many_to_one associations.
* bin/sequel is now more unixy, and can now operate on files or pipes.
* Sequel::Model.plugin can now be overridden just like other model methods.
* Symbol splitting now works with accented characters and kanji characters.

I currently only have a few minor features in mind for 3.22.0.  As always, I'll certainly fix bugs and consider feature requests submitted from the community.
