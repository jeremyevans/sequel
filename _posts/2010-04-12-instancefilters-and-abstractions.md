---
 layout: post
 title: instance_filters and Abstractions
---

I recently <a href="http://github.com/jeremyevans/sequel/commit/0c42c4c63173b4a337ba7f923088bfcb71068d78">added an instance_filters plugin to Sequel</a>, which allows you to add additional filters to the UPDATE or DELETE statements issued when Sequel::Model saves or destroys individual model instances.

# Problem

By default, when Sequel::Model wants to reference an existing model object for updating or deleting, it just uses a filter prepared from the model object's primary key values.  So there wasn't a way to only update in certain cases based on the current value in the database.  In some cases, you really need to be sure that a certain field has a certain value before updating or deleting it.  For example, let's say you only want to allow modifications to records that aren't frozen:

    r = Record(:id=>1, :frozen=>false)
    r.update(:number=>2)
    # UPDATE records SET number = 2
    # WHERE id = 1

This code may look like it will only update an unfrozen record.  However, assuming there is any sort of concurrent access to the database (by other threads or processes), that is not the case.  Between when the record is fetched in the first line, and when it is updated in the 2nd line, it's possible the row in the database could have been modified to freeze the record.  This is a situation where previously, the only safe way to handle it would be to use a dataset:

    Record.
     filter(:id=>1, :frozen=>false).
     update(:number=>2)
    # UPDATE records SET number = 2
    # WHERE id = 1 AND frozen IS FALSE

However, if you had any hooks (which run at the model instance level), they wouldn't be run, and if you want to do more to a record than just update the value of one field, you might be using a lot of niceties that Sequel::Model gives you that aren't present in datasets.

# Solution

This is where the instance_filters plugin comes in.  Now, you can do this:

    r = Record(:id=>1, :frozen=>false)
    r.instance_filter(:frozen=>false)
    r.update(:number=>2)
    # UPDATE records SET number = 2
    # WHERE id = 1 AND frozen IS FALSE

If it turns out that someone changed frozen to false behind your back, Sequel will raise an error.  The way Sequel can detect an error is that delete and update are supposed to return the number of matched rows, and in the number isn't 1, then something problem went wrong.

# Issues

Unfortunately, not all databases and adapters return a correct number when updating or deleting.  Of the adapters I test regularly, the ADO adapter does not work at all, as it always returns nil.  So you would not be able to use the instance_filters plugin on the ADO adapter.  Unfortunately, MySQL uses a slightly different number than other databases and adapters.  Instead of returning the number of matched rows, it returns the number of "affected" rows, which could be less than the number of matched rows if your update statement didn't actually modify the data in the row.  The native mysql adapter and the MySQL do subadapter both suffer from this issue, though the MySQL jdbc subadapter does not.  That means that using this plugin on MySQL using the native or do adapters is a bad idea unless you really know what you are doing, and even then it's subject to possible race conditions.

# Rationale

The idea for the instance_filters plugin came to me after working on the <a href="/2010/02/08/optimistic-locking.html">optimistic_locking plugin</a>.  The optimistic_locking plugin needed exactly what the instance_filters plugin provides, but for a very specific use case.  Specifically, the optimistic_locking plugin needs to ensure that no two separate queries try to update the same row without realizing it.  To do this, it uses a lock column containing an integer representing the current version of the row.  Every time the row is saved, the version is upgraded.  There are two parts to the this.  One is unrelated to instance_filters, which is adding an update to the lock column every time the row is updated.  The other is ensuring that only a row with the matching lock version is affected, which is exactly the job of an instance filter.  So I took the next step and <a href="http://github.com/jeremyevans/sequel/commit/506174d4857b7dfbb6286684da856cf45fe45a16">made the optimistic_locking plugin just use the instance_filter plugin internally</a>.

This method of abstracting out a more specific plugin into a more general plugin has actually been done before in Sequel.  While I was designing Sequel's nested_attributes plugin, to reduce the amount of complexity in the plugin, I abstracted out the instance_hooks plugin, which allows you to add hooks on individual model instances.

# Tangent

It's actually interesting to compare how Sequel handles nested_attributes compared to ActiveRecord.  <a href="http://sequel.jeremyevans.net/rdoc-plugins/classes/Sequel/Plugins/NestedAttributes.html">Sequel's nested_attributes plugin</a> was based in concept on <a href="http://api.rubyonrails.org/classes/ActiveRecord/NestedAttributes/ClassMethods.html">ActiveRecord's nested_attributes support</a>.  Both allow you to set attributes for associated objects directly on the model object.  The implementation is probably significantly different though, as ActiveRecord uses something called <a href="http://api.rubyonrails.org/classes/ActiveRecord/AutosaveAssociation.html">AutosaveAssociation</a>, while Sequel uses the previously mentioned <a href="http://sequel.rubyforge.org/rdoc-plugins/classes/Sequel/Plugins/InstanceHooks.html">instance_hooks plugin</a>.

As it sounds, ActiveRecord's AutosaveAssociation is specific to associations, and handles automatically saving associated records when saving the current record, which is pretty much exactly what nested attributes needs it to do.  However, while it's good for nested_attributes, it's not that generally useful by itself.  Sequel's instance_hooks plugin is just a general framework to add hooks to individual model objects, and the nested_attributes plugin just uses it to save associated objects at the appropriate point (either before saving the current object, or after).  I'm guessing that ActiveRecord's nested attributes internals should be simpler than Sequel's, because AutosaveAssociation is more specific than instance_hooks for the nested attributes use case.  However, I think that instance_hooks is more generally useful than AutosaveAssociation.

For one, ActiveRecord forces you to use AutosaveAssociation association when using nested_attributes, so if can't have the nested attributes behavior without the autosave behavior.  With Sequel's instance_hooks plugin, there is no behavior, so enabling nested_attributes doesn't affect associations that aren't involved in a nested_attributes call.  Also, you can use Sequel's instance_hooks plugin to implement everything AutosaveAssociation can do, but you couldn't use AutosaveAssociation to implement the functionality of instance_hooks.
