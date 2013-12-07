---
 layout: post
 title: Real One-to-One Associations
---

Since 2.1.0, Sequel has supported a :one_to_one option for one_to_many associations, which created a getter that returned just a single object, as well as a setter that allowed updating the associated object.  However, this type of association was always an ugly duckling.  Because it worked well enough, and was simple to implement, I have been relucant to change the situation and create a real one_to_one association.

I should back up a bit and explain the relevant Sequel associations:

* many_to_one: Normal case where you have a foreign key in the current table that points to the primary key in the associated table.  Since it points to the primary key in the associated table, there can be many rows in the current table that point to the same row in the associated table.
* one_to_many: Normal case where you have a foreign key in the associated table that points to the primary key in the current table.  Since the associated table points to the primary key in the current table, there can be many rows in the associated table for each row in the current table.
* one_to_one: Weird case where you have a foreign key in the associated table that points to the primary key in the current table, but you are only interested in a single associated record.   That could be because the foreign key in the associated table is unique, or maybe you want to pick a specific record (e.g. the first matching record for a particular order).

From the definition, you can see that the one_to_many and one_to_one definitions are exactly the same in terms of how things are stored in the database.  The difference is only the number of records you want to return.  Sequel's previous handling of one_to_one associations created a getter that returned the first matching record, and also created a setter that tried to make sure that the passed object was the only object associated to the record.  The one_to_one and one_to_many cases were so similar internally that a real one_to_one association was never created, all one_to_one associations were just one_to_many associations with a couple of methods added and a couple removed.

This worked fine in most cases, but it had a few issues.  First, you had to use the plural association name when defining the association.  Let's say you have songs and lyrics, and all lyrics are associated with a song, but a song may not have a lyric.  For historical reasons, instead of having a song_id column in the lyrics table, there is a lyric_id column in the songs table.  This is the code that you would have used:

    Lyric.one_to_many :songs, :one_to_one=>true
    lyric = Lyric.first
    lyric.song

The first thing that you should notice is you have to use the plural name for the association, since it is still technically a one_to_many association.  This is reflected later, for example if you want to eager load:

    lyrics = Lyric.eager(:songs).all
    lyrics.each{|l| l.song}

See how you had to use the plural of the association for eager loading, even though the singular method is used to return the object?  Similarly, the association reflection required you use the plural form:

    r = Lyric.association_reflection(:songs)

This hampered plugins that used reflection significantly, as you would have had to add special cases to check for one_to_one associations, since the association name did not match the method name.

Well, <a href="http://groups.google.com/group/sequel-talk/browse_thread/thread/9b3dd3153e7f27c5">one Sequel user finally decided it was worth the effort to fix the issue</a>.  For those of you that aren't familiar with his work on Sequel, John Firebaugh is probably the most prolific recent Sequel committer next to me.  His patch solved the initial problem nicely, and with recent patches you can now do:

    Lyric.one_to_one :song
    Lyric.first.song
    Lyric.eager(:song).all{|l| l.song}
    Lyric.association_reflection(:song)

So what's the big deal?  Well, for one, backwards compatibility.  Right of the bat you need to change any code that used the :one_to_one option from using a plural to using a singular.  Since I knew that backwards compatibility had to break without making the internals complex, I decided to take a look at the current code and decide what other backwards incompatible changes should be made, as I don't want to break backwards compatibility more than once.  It turns out that I determined that quite a few changes would be beneficial.  The original patch only changing the naming, all of the internals were the same, and it was still a one_to_many association.  Subsequent changes I made were:

* Adding a real one_to_one association.  So now instead of "one_to_many :songs, :one_to_one=>true", it's just "one_to_one :song".
* one_to_one associated objects are now cached like many_to_one associated objects (either the associated object or nil), rather than like one_to_many associated objects (which are cached as an array of associated objects).  This requires that any custom eager loaders for one_to_one associations be modified beyond just changing the association name.
* Previously, there was an internal private add_ association used by the one_to_one association setter.  That has been removed, and now the association setter uses its own code.  This code is similar to the many_to_one setter, where there is a public setter method that calls a generic method (set_one_to_one_associated_object) which calls the private _setter method.
* The association_dataset method for one_to_one associations was previously private, but now it is public just like it is for other associations.
* Instead of raising an error if multiple objects are returned, the one_to_one association now limits itself to a single object (LIMIT 1).

Because of all the backwards incompatible changes, I decided to make one final backwards incompatible change, which is to make the previous way of defining one_to_one associations raise an error.  I did this because I think it's superior to raise an error immediately when the association is defined rather than later when an attempt to access the association is made.

During this process I made some other improvements as well, unrelated to backwards compatibility:

* You can now assign nil to a one_to_one setter, before this raised an error.
* When the one_to_one setter is used where another object is already associated with the current object, Sequel now will disassociate the record currently associated before associating the new record, which can fix some errors related to uniqueness checks.
* You should be able to eagerly load nested associations when lazily loading both many_to_one and one_to_one associations.
* Eagerly loading a one_to_one and a single *_to_many association with eager_graph now no longer eliminates duplicate records unnecessarily.
* Using the many_to_one setter when the reciprocal association is a one_to_one association and the argument is an object with a currently different associated object no longer raises an error.
