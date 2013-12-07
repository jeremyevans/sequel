---
 layout: post
 title: Optimistic Locking
---

Today I committed an <a href="http://github.com/jeremyevans/sequel/raw/master/lib/sequel/plugins/optimistic_locking.rb">optimistic locking plugin for Sequel::Model</a>, modeled on the <a href="http://api.rubyonrails.org/classes/ActiveRecord/Locking/Optimistic.html">optimistic locking support in ActiveRecord</a>.  Usage is fairly simple, and it defaults to the same locking column as ActiveRecord, for ease of migration:

    class Person < Sequel::Model
      plugin :optimistic_locking
    end
    p1 = Person[1]
    p2 = Person[1]
    p1.update(:name=>'Jim') # works
    p2.update(:name=>'Bob') # raises Sequel::Plugins::OptimisticLocking::Error

Optimistic locking works fine in most web applications, as long as you don't mind retrying updates if an optimistic locking error is raised.

Optimistic locking is one of the last significant features that was supported by default in ActiveRecord that Sequel didn't already support natively or via a plugin.  I think observers may be the last remaining significant feature, but I'm not sure Sequel should support them, as they don't accomplish anything that can't be supported via a regular plugin.  If you'd like to see Sequel support observers, please post in the comments with your reasons.
