---
 layout: post
 title: Sequel 4.32.0 Released
---

Sequel 4.32.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_32_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A no_auto_string_literals extension has been added, preventing some common SQL injection vectors.
* one_through_one associations now support a setter method.
* Model.default_association_options can be used to specify defaults for association options.
* The tactical_eager_loading_plugin can now load dependent associations eagerly.
