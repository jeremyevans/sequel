---
 layout: post
 title: Sequel 3.34.0 Released
---

Sequel 3.34.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_34_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Added pg_array and pg_array_ops extensions for dealing with PostgreSQL arrays.
* Added pg_hstore and pg_hstore_ops extensions for dealing with PostgreSQL hstores.
* Added pg_auto_parameterize and pg_statement_cache extensions for automatically parameterizing and preparing queries.
* Added query_literals extension for more easily using literal strings with placeholders in select/group/order methods.
* Added select_remove extension for removing selected columns/expressions from a dataset.
* Added schema_caching extension for writing and reading schema metadata from a file instead of parsing it from the database.
* Added null_dataset extension to return a dataset that will not issue a database query.
* Added static_cache plugin for caching an entire model staticly.
* Added many_to_one_pk_lookup plugin for speeding up most many_to_one association lookups.
* Added replacements for most of Sequel's core extensions to the Sequel module.
* Expanded virtual row support to include operators and literal strings.
* Added the ability to require sequel/no_core_ext to require Sequel without the core extensions.
* Supported foreign keys in the schema dumper.
* Added Dataset#to_hash_groups for returning a hash categorized by a field, with an array of all matching values.
* Model#set_fields and #update_fields now support :missing=>:skip and :missing=>:raise options.
* Added Database#drop_table? for only dropping a table if it already exists.
* Added Database#create_join_table for easily creating many_to_many join tables.
* Added Model#freeze for making an model instance read-only.
* Made numerous performance improvements, including doubling the speed of model lookups by primary key.
* Deprecated support for Ruby <1.8.7, PostgreSQL <8.2, and disable_insert_returning on PostgreSQL.
