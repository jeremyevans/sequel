---
 layout: post
 title: Dynamic Association Modification
---

For almost three years (since version 2.2.0), Sequel has had the most flexible and powerful associations of any ruby ORM.  I'm still not aware of another ruby ORM that allows the extent of customization of eager loading that Sequel has provided since 2.2.0.

While Sequel associations have been flexible and powerful, they haven't been dynamic.  You could customize the loading any way you want, but those customizations were set in stone, usually at class definition time, and they weren't modifiable on the fly.

This isn't so much a problem for regular association loading, since the association dataset method has existed for a long time that gives you direct access to
the dataset, allow you to customize it to your desire:

    Artist.one_to_many :albums
    artist = Artist.first
    artist.albums_dataset.filter(:name.like('%foo%')).all

However, using the association dataset method ignores the caching, callbacks, and reciprocal handling done by default.  A recent commit allows you to do the same type of <a href="https://github.com/jeremyevans/sequel/commit/3b633ee373c6e2e922ed68138f3ac27bc6bf70ae">dynamic customizations on the dataset while still handling caching, callbacks, and reciprocals</a>.  To do so, you pass a block to the association method:

    artist.albums{|ds| ds.filter(:name.like('%foo%'))}

Of particular importance is the reciprocal handling, as:

    artist.albums_dataset.filter(:name.like('%foo%')).all.each do |a|
      a.artist.name
    end

Causes a query for every matching album to get the artist, while:

    artist.albums{|ds| ds.filter(:name.like('%foo%'))}.each do |a|
      a.artist.name
    end

does not cause any queries to get the artist, since the reciprocal association is set.

While dynamic regular association loading is certainly helpful, it's just more convenient, it doesn't really add new features.  The big news is that eager loading via both eager and eager_graph got the same dynamic treatment, allowing you to make custom modifications to the eagerly loaded dataset at query time.  For example, let's say you want to eagerly load the albums with names containing 'foo' for all artists:

    Artist.eager(:albums=>proc{|ds| ds.filter(:name.like('%foo%'))}).all

The big news here is the customization can vary per call, for example, based on user submitted data.  I'm not aware of any other ruby ORM with a similar feature.

Sequel doesn't stop there, it allows you to combine dynamic customization with cascading.  So if you wanted to also eagerly load the tracks for all albums with names containing 'foo' for all artists, you could do:

    Artist.eager(:albums=>{proc{|ds| ds.filter(:name.like('%foo%'))}=>:tracks}).all

And dynamic customizations can be done at multiple levels, so if you only wanted to eagerly load the tracks with names containing 'bar':

    Artist.eager(:albums=>{proc{|ds| ds.filter(:name.like('%foo%'))}=>
      {:tracks=>proc{|ds| ds.filter(:name.like('%bar%'))\}\}}).all

This idea originally came from <a href="http://groups.google.com/group/sequel-talk/browse_thread/thread/73bbd3c8a6c8355d">a suggestion by John Firebaugh on the Sequel Google Group</a> and he also did most of the work to implement it.
