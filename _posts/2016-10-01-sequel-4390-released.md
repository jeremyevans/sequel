---
 layout: post
 title: Sequel 4.39.0 Released
---

Sequel 4.39.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_39_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Database#rollback_checker has been added, returning a callable that returns whether the transaction was rolled back.
* Database.set_shared_adapter_scheme has been added, allowing external shared adapters to easily support the mock adapter.
* The hook_class_methods and active_model plugins no longer keep all model instances in memory until transaction commit.
* PostgreSQL 9.6 ADD COLUMN IF NOT EXISTS, jsonb_insert, and full text phrase searching are now supported.
* Sequel.[] has been added as an alias for Sequel.expr.
