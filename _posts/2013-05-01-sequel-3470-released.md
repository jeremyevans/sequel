---
 layout: post
 title: Sequel 3.47.0 Released
---

Sequel 3.47.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_47_0_txt.html">Full release notes are available</a>, but here are some highlights:

* An auto_validations plugin has been added, for automatically adding not null, type, and uniqueness validations.
* An input_transformer plugin has been added, for preprocessing input to model column setters.
* Database.extension has been added, for loading extensions into all future Database instances.
* Model#modified! and #modified? now accept an optional column argument.
* The pg_array extension now allows for Database instance specific array types.

Sequel 4 implementation will begin shortly.  There is still time to <a href="https://github.com/jeremyevans/sequel-4-plans">review and provide feedback on the implementation plan</a>.
