---
 layout: post
 title: Hacker News
---

<a href="http://news.ycombinator.com/item?id=1140666">Sequel got mentioned on Hacker News today</a>
.The comments were overall very positive.  I'm going to address a couple things that people mentioned:

First, Sequel's name.  Personally, I like it,  Yes, googling just Sequel gives mostly unrelated material, but <a href="http://news.ycombinator.com/item?id=1141099">as bk mentions</a>, if your query contains both ruby and sequel, the results are pretty good.

Next, <a href="http://news.ycombinator.com/item?id=1141695">j_baker seems to think that Sequel's slogan ("the database toolkit for ruby"), is somehow related to object relational mapping</a>.  While ActiveRecord is mostly just an ORM, as is Sequel::Model, Sequel core is not an ORM at all.  I think a database toolkit actually describes Sequel, as it is a general tool that supports database access.  What makes Sequel neat is that it represents SQL queries as objects with a functional method-chaining API, and has a very user friendly DSL that allows the creation of advanced database-indepedent queries.

<a href="http://news.ycombinator.com/item?id=1140936">tibbon wishes that there was a good page explaining why/how Sequel is different from ActiveRecord</a>.  There currently isn't a page on <a href="/">Sequel's site</a>, as it's not really focused on advocacy.  <a href="http://sequel.rubyforge.org">Sequel's site</a> describes mostly how to use Sequel, rather than the differences between it and ActiveRecord.  However, I'm sure that I'll be adding more blog posts in the future highlighting differences between Sequel and ActiveRecord.

<a href="http://news.ycombinator.com/item?id=1141443">mjw likes Sequel, but wants to see more SQLAlchemy-like features</a>.  Sequel is very similar to SQLAlchemy (slogan: "the database toolkit for python") in terms of power (and with much more elegant syntax thanks to ruby), but one thing it lacks is the "Unit of Work" pattern where you make a bunch of changes to an object graph and have the library figure out the changes and apply all of them in a single transaction.  With Sequel, you'd open a transaction yourself, and then make changes to all objects inside that transaction.  Personally, I haven't needed the "Unit of Work" pattern or felt that it adds much value, and Sequel isn't really designed for it, so it's unlikely that it will be added.
