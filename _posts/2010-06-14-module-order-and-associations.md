---
 layout: post
 title: Module Order and Associations
---

Sequel::Model uses modules for pretty much all of its methods. All Sequel::Model plugins contain 1-3 modules for class, instance, and dataset methods.  Sequel::Model itself is a plugin, as is Sequel::Model::Associations.  All plugins that add class-specific methods add them to modules included in the class instead of the class itself.  The reason for using modules for everything is that it allows you to override any method that you want and still call super to get the default behavior.

While Sequel::Model's use of modules works great, there was a corner case involving method lookup order that resulted in a situation where an undesired method was chosen.  This could happen in the following case:

1. You have a Sequel::Model subclass
2. You load a plugin using the plugin method
3. The plugin adds an association to the model
4. The plugin also requires another plugin that defines an instance method with the same name as the association

The end result of this issue is that calling the association method will call the instance method defined in the dependent plugin, and not the association method in the original plugin.  This is due to how Sequel handles association methods.

Sequel puts association methods in the same module it puts column methods, which is an anonymous module included in the class.  So ruby's method lookup for a model subclass instance, the situation looks something like this (assuming a model named Album):

1. Album
2. <Module:0x0> (column/association module)
3. Sequel::Model
4. Sequel::Model::Associations::InstanceMethods
5. Sequel::Model::InstanceMethods

When you add plugin A that depends on plugin B, where A defines an association and B defines as an instance method with the same name, the situation becomes:

1. Album
2. Sequel::Model::Plugins::B::InstanceMethods
3. <Module:0x0> (column/association module)
4. Sequel::Model
5. Sequel::Model::Associations::InstanceMethods
6. Sequel::Model::InstanceMethods

The important thing to note is that plugin B's instance methods come before the column/association methods, which is where plugin A added the association.  In this case, A wants the association to override the instance method defined by B, but that's not the case.

To fix this situation, I chose to make it possible to choose into which module association methods get placed, via <a href="http://github.com/jeremyevans/sequel/commit/79089fde6499d3fbfa41323cd8efd900a8396e9c">the :methods_module association option</a>.  So to fix the situation, plugin A needs to require plugin B, and after requiring it, needs to create an anonymous module, include that in Album, and use it as the :methods_module association option for the associations it creates.  That changes the method lookup to be:

1. Album
2. <Module:0x0> (Plugin A association module)
3. Sequel::Model::Plugins::B::InstanceMethods
4. <Module:0x0> (column/association module)
5. Sequel::Model
6. Sequel::Model::Associations::InstanceMethods
7. Sequel::Model::InstanceMethods

This makes the association methods defined in plugin A override the instance methods defined in plugin B, which is what we want.

I only discovered this scenario when I found myself in the situation described here, where I wanted the rcte_tree plugin (which defines an ancestors association) to depend on the newly added tree plugin (which defines an ancestors instance method), and I wanted the rcte_tree associations to take precedence over the tree instance methods.  With the :methods_module association option, all it took was <a href="http://github.com/jeremyevans/sequel/commit/d8a970b7743c0a074d632e6147c023c8b88082b4#L1R102">two extra lines in the rcte_tree plugin</a> and the method lookup order was fixed.
