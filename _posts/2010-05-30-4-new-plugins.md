---
 layout: post
 title: 4 New Plugins
---

A few days ago <a href="http://github.com/jeremyevans/sequel/commits/8441a1e6d4f2208488159756678ec505bb1e06f9">I committed 4 new plugins to Sequel</a>.  Most of these plugins were small, two just modify a single method.

### <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/association_pks.rb">association_pks</a>

association_pks is the most extensive plugin of the four.  It adds association_pks and association_pks= methods to one_to_many and many_to_many associations, similar to the association_ids and association_ids= methods for ActiveRecord's associations.  These methods operate at the dataset level, so they avoid the creation of model objects.  A consequence of this is that the association_pks= method does not call any add or remove association callbacks, so if you have association callbacks and want them to be executed, you probably don't want to use this plugin.

### <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/update_primary_key.rb">update_primary_key</a>

The update_primary_key plugin name is a bit misleading, as by itself it doesn't have much to do with updating.  However, the reason it is named that way is that it allows you to update the primary keys in your model objects and use the cached value of the primary key when saving.  All it does by itself is cache the pk_hash value for model objects right when they are loaded and after they are updated.

This plugin should only be necessary if you are using natural keys instead of surrogate keys, and you need to modify the value of one of the primary keys.

### <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/string_stripper.rb">string_stripper</a>

The string_stripper plugin is very simple.  All it does is strip strings assigned to model objects.  This is helpful if you are using Sequel on a website and want to remove leading and trailing whitespace from all form input.

### <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/skip_create_refresh.rb">skip_create_refresh</a>

skip_create_refresh is another really simple plugin.  All it does is skip the refresh when a new model object is saved.  Sequel refreshes by default in order to get the values of all of the columns.  This is mainly a performance booster if you don't care about the values of other columns.
