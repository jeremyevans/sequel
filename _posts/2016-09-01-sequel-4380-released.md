---
 layout: post
 title: Sequel 4.38.0 Released
---

Sequel 4.38.0 was released today!  <a href="/rdoc/files/doc/release_notes/4_38_0_txt.html">Full release notes are available</a>, but here are some highlights:

* Ruby's coercion protocol is now supported for numeric expressions, allowing code such as Sequel.expr{1 - a}.
* Sequel now supports the ** method on many expressions for exponentiation.
* Sequel::SQLTime.date= has been added to set the date to use for instances.
* Database after_commit/rollback hooks are only added when saving model instances if the model instances override the default methods.
