---
 layout: post
 title: Composition Plugin Added
---

I recently added <a href="http://github.com/jeremyevans/sequel/commit/86db4dc744d0baa710ee766773f6cdf72c0b1b09">a composition plugin to Sequel</a>, similar to <a href="http://api.rubyonrails.org/classes/ActiveRecord/Aggregations/ClassMethods.html">ActiveRecord's composed_of method</a>.  Both Sequel's new composition plugin and ActiveRecord's composed_of do basically the same thing: automatically create getters and setters that return composition objects where the data for the composition objects comes from the model object's columns' values.

A simple example for where this plugin would be helpful is if you have a legacy database that stored dates using three columns, one each for year, month, and day.  In ruby, it's easier to just use a single Date object instead of attempting to manipulate year, month, and date individually.  With Sequel's composition plugin, this is as easy as:

    class Event < Sequel::Model
      plugin :composition
      composition :date, :mapping=>[:year, :month, :day]
    end

The way the plugin works is that when you call Event#date, it will return a ruby Date object with the same year, month, and day as that particular event's year, month, and day columns.  If you later change the date via Event#date=, when you save the event, it will set the year, month, and day columns of the event based on the date passed to Event#date=.

So ActiveRecord and Sequel have the same general idea, but there are quite a few differences.  The first difference is that ActiveRecord makes a big deal about how the composed objects should be "value objects" which have the properties of being immutable and interchangeable.  Even though using value objects is a good practice, Sequel will work just fine with objects that aren't value objects.  Sequel's implementation of the composition plugin always takes the composition object values and writes them back to the object, even if you don't call the composition setter method, so modifying the composition object itself is not problematic.  However, if you do use mutable composition objects and you only want to save changes to records, you should explicitly mark the object as modified with Model#modified!.

Both Sequel and ActiveRecord support custom composition object creation procs.  ActiveRecord uses the :constructor option for this, Sequel uses the :composer option.  ActiveRecord uses a regular proc that takes arguments specified by the :mapping option (the same number and order), while Sequel uses an instance evaled proc.  ActiveRecord doesn't allow for a custom decomposition proc (which modifies the model object's columns based on the composition object), while Sequel does via the :decomposer option. In ActiveRecord, the :mapping option isn't optional (I think, if I'm wrong please let me know), while in Sequel it is optional.  The only purpose of the :mapping option in Sequel is for ease of use, as all of the work is done by the :composer and :decomposer procs.  If you provide a :mapping option and a :composer or :decomposer option is not provided, Sequel will create an appropriate proc for you based on the :mapping option.

ActiveRecord supports a custom :convertor proc that is used by the composition object setter if it is not already in the correct class.  Sequel takes a duck-typing approach to this and allows you to assign any value to the composition object setter.  If you really want to enforce a certain class, you can override the composition object setter in the class, check the class of the method argument, and call super with either that object or a different object.

ActiveRecord also supports an :allow_nil option that can be set to false to not instantiate the object if all backing columns are nil.  That's the default behavior of Sequel if you use the :mapping option and don't define a custom :composer or :decomposer.  If you want different behavior, you can always provide your own :composer and :decomposer.

Similar to associations, ActiveRecord uses :class_name where Sequel uses :class, and Sequel's :class option can take a Class, Symbol, or String (I think ActiveRecord requires a String).

Sequel instantiates the composition object lazily when you call the composition getter method, while I believe ActiveRecord does so on object initialization.

There are probably some other minor differences as well, such as ActiveRecord allowing symbols specifying methods instead of procs, how compositions with the same name as the backing columns are handled, and how compositions using virtual attributes are handled.

Sequel's new composition plugin hopefully will make it easier for ActiveRecord users to transition to Sequel.  If you are a current ActiveRecord user that uses composed_of, please reply in the comments and let me know if Sequel's composition plugin will meet your needs.
