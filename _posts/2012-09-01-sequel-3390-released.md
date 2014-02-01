---
 layout: post
 title: Sequel 3.39.0 Released
---

Sequel 3.39.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_39_0_txt.html">Full release notes are available</a>, but here are some highlights:

* A constraint_validations extension and plugin have been added for validations enforced by database constraints.
* Dataset#count now accepts an argument to easily allow count(expression)
* Database#copy_into has been added to the postgres adapter for very fast inserts into tables using COPY.
* Sequel now parses current date/timestamp default column values fro parsing the schema for a table.
* On MySQL and PostgreSQL, Sequel can now combine multiple alter_table operations into a single query.
