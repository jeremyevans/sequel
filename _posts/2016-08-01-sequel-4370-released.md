---
 layout: post
 title: Sequel 4.37.0 Released
---

Sequel 4.37.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_37_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Native prepared statements and bound variables are now supported when using mysql 0.4+.
* The connection pools now handle disconnect errors not explicitly raised as Sequel::DatabaseDisconnectError.
* Regular expressions in dataset filters are now supported on Oracle 10g+.
* Database#values has been added on SQLite 3.8.3+ to support the VALUES clause.
