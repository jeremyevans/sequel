---
 layout: post
 title: Sequel 3.10.0 Released
---

Sequel 3.10.0 was just released!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_10_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A real one_to_one association was added (using one_to_many with the :one_to_one option now raises an exception).
* many_to_one and one_to_one associations now use before_set and after_set callbacks.
* "Pessimistic Locking" is now supported via Dataset#for_update and Model#lock!.
* A composition plugin was added, similar to ActiveRecord's composed_of.
* An rcte_tree plugin was added, allowing the loading of all ancestors and descendants in fast recursive common table expression queries.

One of the main features planned for 3.11.0 is going to be an overhaul of the logging support, so that you can only log queries that raise errors or take a long time.
