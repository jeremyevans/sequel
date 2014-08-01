---
 layout: post
 title: Sequel 4.13.0 Released
---

Sequel 4.13.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_13_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A modification_detection plugin has been added for detecting in-place modifications.
* A column_select plugin has been added for explicit column selections.
* An insert_returning_select plugin has been added for using INSERT RETURNING when selecting specific columns.
* A pg_enum extension has been added for dealing with PostgreSQL enum types.
* A round_timestamps extension has been added for rounding timestamps to database precision.
* A dataset_source_alias extension has been added for aliasing datasets to their first source.
* RETURNING is now emulated with OUTPUT on Microsoft SQL Server.
