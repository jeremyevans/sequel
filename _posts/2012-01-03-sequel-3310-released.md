---
 layout: post
 title: Sequel 3.31.0 Released
---

Sequel 3.31.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_31_0_txt.html">Full release notes are available</a>, but here are some highlights:

* The serialization plugin now accepts custom serialization formats.
* Dataset #import/#multi_insert can now return an array of inserted primary key values.
* You can now have a many_to_one assocation with the same name as the foreign key column.
* GROUP BY ROLLBACK/CUBE is now supported via the Dataset #group_rollup and #group_cube options.
* Dataset #exists and #full_text_search now work with prepared statement placeholders.
