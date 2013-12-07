---
 layout: post
 title: Sequel 3.33.0 Released
---

Sequel 3.33.0 was released today!  <a href="https://raw.github.com/jeremyevans/sequel/master/doc/release_notes/3.33.0.txt">Full release notes are available</a>, but here are some highlights:

* A server_block extension has been added that scopes database access inside a block to a given server/shard.
* An arbitrary_servers extension has been added that allows you to connect to arbitrary shards/servers.
* You can now use 1/0 for booleans in the sqlite adapter, instead of 't'/'f'.
* You can now disable transaction use in migrations on a per-migration basis.
* Foreign key creation now works without specifying the primary key column manually on MySQL/InnoDB.
