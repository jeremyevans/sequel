---
 layout: post
 title: STI Improvements
---

Sequel::Model's single table inheritance (STI) support was originally added by me <a href="http://github.com/jeremyevans/sequel/commit/cc7bbe812a2e50a5b2293be13301ed3cd7318bd9">back in June 2008</a>.  Like quite a few other things, <a href="http://github.com/jeremyevans/sequel/commit/6f317238af1571125d4e6c2326c5cef60ed38f07">it was moved to a plugin</a> in Sequel 3.0.0.  Since then, there have been a couple of minor bug fixes, but no significant feature improvments.

After some initial discussions and some code by <a href="http://github.com/tmm1">Aman</a>, I began work on a significant increase in functionality to the STI plugin.  Aman wanted to use the STI plugin with an integer field, where the integers should be mapped to specific subclasses.  Sequel's STI plugin didn't support that, but it wasn't that hard to add.  It took some more work to make it so that in addition to instantiating the correct subclass, it used the correct filter in the subclasses and set the class discriminator column to the correct value when creating objects.  After that was done, the final part was to make a nice API so that the user only has to specify the minimum amount of information to make it work, but can also have complete control.  Thankfully, I think I accomplished the goal, as the STI plugin is now both <a href="http://github.com/jeremyevans/sequel/commit/799a2090ce51c0f504594c911cd46c252db1d9a0">simple and flexible</a>.

Let's jump into some usage examples.  First, the plugin is still backwards compatible, so the minimum requirement is just specifying the class discriminator column:

    Employee.plugin :single_table_inheritance, :kind
    
Second, to handle simple mapping needs like Aman has, you can use a :model_map option with a hash value.  The :model_map option should have keys that are column values, and values that are Symbols or Strings of class names.  Sequel needs to do the mapping two ways, so to get the mapping the other way, Sequel inverts and processes the :model_map.

    Employee.plugin :single_table_inheritance, :type,
      :model_map=>{1=>:Staff, 2=>:Manager}
    
Finally, Sequel allows full control by letting you use procs for both the :model_map and its counterpart the :key_map.  A :model_map proc takes a column value and returns a class or class name as a symbol or string, and the :key_map proc takes a class object and should return the column value to use.  Here's an example where you are storing the reverse of the class name in the column:

    Employee.plugin :single_table_inheritance, :type,
      :model_map=>proc{|v| v.reverse},
      :key_map=>proc{|klass| klass.name.reverse}

After adding this ability, I decided to remove a long standing limitation in the STI plugin, which is that it could previously only handle a 2-level class hierarchy.  So if you had Manager as a subclass of Employee and Executive as a subclass of Manager, Sequel would not return any executives if you asked for all managers.  <a href="http://github.com/jeremyevans/sequel/commit/b65beb2d24fba9bcc4496df12e59c0fb07f90461">That limitation has now been removed</a>, so there is currently no limit to the depth of an STI class hierarchy.
