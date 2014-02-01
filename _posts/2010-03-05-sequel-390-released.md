---
 layout: post
 title: Sequel 3.9.0 Released
---

Sequel 3.9.0 was released yesterday!  <a href="/rdoc/files/doc/release_notes/3_9_0_txt.html">Full release notes are available</a>, but here are some highlights:

* The connection pool classes were refactored for a 25-30% performance increase.
* An optimistic locking plugin similar to ActiveRecord's optimistic locking was added.
* The ability to create unused aliases similar to Arel was added.
* NOT IN with an empty array is now handled properly.
* Schemas and aliases now work with eager graphing.

I'm already working on some interesting patches that will be in 3.10.0, expect future blog posts to cover them.
