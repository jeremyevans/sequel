---
 layout: post
 title: Sharding for Models
---

<a href="http://github.com/jeremyevans/sequel/commit/7aeea22dd348f55341cb2bb99b462ee6d5ab564d">Sequel added sharding support back in August 2008</a>, and still remains the only ruby database library with built-in sharding.  There haven't been extensive changes in Sequel's sharding since its release, and the only signficant feature was added in December 2009, <a href="http://github.com/jeremyevans/sequel/commit/adf6a891a52f28a7012d8534799ce8fab1c24aaf">allowing easy access to all of the Database object's shards via each_server</a>.

While Sequel has supported sharding for a long time, it has previously only supported it at the dataset level.  There wasn't built in support for sharding in models, until earlier this week when I <a href="http://github.com/jeremyevans/sequel/commit/ba2194dc483e893ddbb118e7c66a945bf347b0f8">added the sharding plugin as one of the built in model plugins.</a>

The reason that sharding plugin wasn't added sooner is that Sequel does not provide an easy way to determine which dataset was used to retrieve a model object.  The <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/tactical_eager_loading.rb">tactical eager loading plugin</a> can do so, but only when you use Dataset#all to retrieve all model objects at once.  This works fine for eager loading, since you need all model objects at once anyway, but it won't work for other cases such as retrieving a single record or iterating over the result set with each or one of the other Enumerable methods.

The sharding plugin uses an approach I've known about for a couple years, but never thought to apply before.  While there isn't a way for a model object to know what dataset was used to retrieve it, that's not actually necessary for sharding support in models.  The only thing the model object needs to know is which shard it was retrieved from, not which dataset.  So the technique the sharding plugin uses is to override Dataset#server for the model's dataset.  If that dataset has a row_proc (which most model datasets do), it overrides the row_proc with a new proc that calls the previous row_proc (which transforms the hash into a model object), and then calls set_server on the model object with the same symbol that was passed to the server method.

With that method, now every retrieved model object knows which shard it was retrieved from, so the trick is to just use that information to make sure that the model object uses that shard when it interacts with the database.  It turns out that that happens in a few different places:

1. When refreshing the object
2. When updating the object
3. When deleting the object
4. When dealing with the object's associations

The first three were fairly easy to handle, but the fourth took a little bit of refactoring in the standard associations plugin.  However, all cases of standard loading of associations are now handled, as well as the add\_/remove\_/remove\_all\_ methods.  Eager loading is not currently handled, as it requires additions to the eager_loader API to let eager loaders know what the current dataset is, so they can check for use of a specific shard, and use that shard when eagerly loading.  Eager loading via eager_graph is also not handled currently, but will probably be added when the other eager loading change is made.

In addition to dealing with existing model objects, there needs to be an easy way to create new model objects on a specific shard, so the sharding plugin adds the new_using_server and create_using_server class methods for that purpose.
