---
 layout: post
 title: Sequel 3.29.0 Released
---

Sequel 3.29.0 was released today!  <a href="http://sequel.jeremyevans.net/rdoc/files/doc/release_notes/3_29_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Oracle support has been improved greatly.
* Support has been added for the HSQLDB and Apache Derby Java databases.
* The db2 adapter has been improved significantly.
* A mock adapter has been added for better testing support.
* Many transaction related features have been added, such as after commit/rollback hooks.
* A dataset_associations plugin has been added, allowing association methods to be called on datasets.
* Database#extend_datasets has been added, allowing you to fully customize the datasets for a database.
* Database#timezone has been added, so you can set Sequel.database_timezone per database.
* Numerous optimizations have been made to speed up loading of model objects from the database.
