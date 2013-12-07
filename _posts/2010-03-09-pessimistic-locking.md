---
 layout: post
 title: Pessimistic Locking
---

About a month after <a href="/2010/02/08/optimistic-locking.html">optimistic locking was added as a model plugin</a>, Sequel added the equivalent of ActiveRecord's <a href="http://api.rubyonrails.org/classes/ActiveRecord/Locking/Pessimistic.html">pessimistic locking</a>, via support for <a href="http://github.com/jeremyevans/sequel/commit/e8934fc845d7c9be5148de96ee432c0e0b27ced9">Dataset#for_update</a> and <a href="http://github.com/jeremyevans/sequel/commit/f859da32b3ccbd93b20d19569ed252676bbb6843">Model#lock!</a>.

Pessimistic locking is significantly different from optimistic locking.  Optimistic locking assumes that records will not be modified simultaneously, and just raiises an error if it detects that situation.  Pessimistic locking sends a query to the database telling it to lock a certain row so that other connections cannot modify that row.  Pessimistic locking is only useful inside a transaction, and the lock is released when the transaction completes.  If another connection attempts to modify the locked row, it blocks until the connection that locked the row completes the transaction.

There are two ways to use the pessimistic locking support in Sequel.  The first and recommended way is to use Dataset#for_update:

    Album.db.transaction do
      album = Album.for_update.first(:id=>1)
      # album's row is locked for updating when retrieved

      album.update(:name=>'RF')
    end

You can do something similar using Model#lock!:

    Album.db.transaction do
      album = Album[1]
      # album's row is not locked for updating

      album.lock!
      # album's row is now locked for updating

      album.update(:name=>'RF')
    end

The Dataset#for_update method is recommended in most cases, as Model#lock! requires sending an additional query for the database for every row.  However, Model#lock! can be helpful in certain situations.  For example, if you are retrieving 100 records and may only want to update a few, it might be better to not use for_update, as that would lock all 100 rows, and instead use Model#lock! to lock only those rows that need to be updated.

Note that using pessimistic locking in multithreaded applications with certain underlying drivers is a bad idea.  Some drivers, such as ODBC and the default MySQL driver, block the entire ruby interpreter when sending queries.  On those drivers, if you are using two separate threads that both try to lock the same record, your application will freeze, as the thread that blocks waiting for the other thread to finish the transaction will block the entire ruby interpreter, and the other thread will never run.  To be fair, this situation can also happen without pessimistic locking, but the window for the race condition is smaller.  The only safeway to handle things is to do one of the following:

* Run single threaded
* Use a driver that doesn't block the interpreter (e.g. ruby-pg)
* Use JRuby or another ruby interpreter with better threading support
