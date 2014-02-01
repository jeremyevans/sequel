---
 layout: post
 title: Sequel 3.35.0 Released
---

Sequel 3.35.0 was released today!  <a href="/rdoc/files/doc/release_notes/3_35_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Many thread-safety issues were fixed on ruby implementations that don't use a global interpreter lock.
* A dirty plugin was added for getting original values of columns after changing the columns.
* Database#create_table now respects an :as option for creating a table from a dataset.
* The features deprecated in 3.34.0 have been removed.
