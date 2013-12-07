---
 layout: post
 title: require_modification
---

I recently noticed the following Sequel behavior:

    DB.create_table(:as) do
      primary_key :id
      String :name
    end
    class A < Sequel::Model; end
    a = A.create(:name=>'J')
    a.delete
    a.save # no error!
    a.delete # no error!

This smells wrong.  If the object has been deleted, how can you successfully save it or delete it a second time?

When you actually look at what queries it sends the database, it's easy to see why no error is raised:

    a.delete
    # DELETE FROM as WHERE id = 1
    a.save
    # UPDATE a SET name = 'J' WHERE id = 1
    a.delete
    # DELETE FROM as WHERE id = 1

Note that there is nothing wrong with any of these three queries.  The only difference between the first one and the last two is that in the first query, the row with id 1 exists, and in the last two it does not.

Now, you can try to cheat, by having the delete method set a flag that will raise an error if you later attempt to save or delete that object.  However, while that fixes the issue with the above code (which nobody would do in practice), it's still vulnerable to an obvious race condition, since the row could be deleted from the database between the time the row was retreived and the time it is saved or deleted.

With recent commits, Sequel attempts to take the robust approach, <a href="http://github.com/jeremyevans/sequel/commit/21ef8d436cac677c4dde21bf9b210d761dbd7671">by checking the number of rows matched by the delete or update statements</a> (which most adapters return), and raises an error if the number is not 1 (since the ActiveRecord pattern that Sequel follows maps each Sequel::Model instance to a single database row). 

Unfortunately, not all adapters support this.  As mentioned in a recent post, the ADO adapter, the native MySQL adapter, and the MySQL do subadapter all do not support this, so it is not enabled by default when you are using one of those adapters.  You can enable it or disable it like most other Sequel::Model flags:

    Sequel::Model.require_modification = false # global
    Album.require_modification = true # class
    album.require_modification = false # instance

Unless enabled or disabled at a global level, <a href="http://github.com/jeremyevans/sequel/commit/ddccd464714df454b90151ce0e0ac28cc6b3dd82">model classes will check for support for accurate rows matched numbers</a> on their dataset when deciding whether to enable this.
