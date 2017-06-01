---
 layout: post
 title: Sequel 4.47.0 Released
---

Sequel 4.47.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_47_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Database#with_server in the server_block extension now supports a second argument for the read only default server.
* Model.default_association_type_options has been added for custom defaults per association type.
* Database#views on PostgreSQL now supports a :materialized option to return materialized views.
