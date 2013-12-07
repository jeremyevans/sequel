---
 layout: post
 title: rcte_tree plugin
---

Yesterday I added a new plugin to Sequel, <a href="http://github.com/jeremyevans/sequel/commit/4ca07d9336bc94643654839e932124a3a6807f49#diff-1">rcte_tree</a>.  Originally, I wasn't sure if it was a good fit to be shipped with Sequel.  Most plugins shipped with Sequel are very general purpose and can be used in a wide variety of situations, while the rcte_tree plugin depends on specific database support and solves a problem that most users don't have.  However, <a href="http://groups.google.com/group/sequel-talk/browse_thread/thread/5fd13ef27e8582bd">based on feedback from Sequel users on the Google Group</a>, I decided to ship it with Sequel.

So what does the rcte_tree plugin do?  The main feature of the plugin is that is uses a recursive common table expression to load all ancestors and descendants for a given node in a tree structure in a single query.  It can also eagerly load all descendants of a group of nodes to a given level (level 1 being children, 2 being children and grandchildren, etc.).  In addition, it adds standard parent and children associations, and the ancestors and descendants associations also preload the parent and children associations for all returned records.  Like most Sequel plugins, it's highly configurable, so you can change the names of the associations as well as which columns to use as the foreign key and primary key.

For a while, there has been an example in <a href="http://sequel.jeremyevans.net/rdoc/files/doc/advanced_associations_rdoc.html">Sequel's Advanced Associations page</a> that gave an example of loading all descendants in a tree structure using a recursive common table expression.  However, earlier this month I came across the <a href="http://explainextended.com/">Explain Extended</a> blog, with <a href="http://explainextended.com/2009/09/24/adjacency-list-vs-nested-sets-postgresql/">a particularly interesting post comparing loading ancestors and descendants using the nested set model and the adjacency list model</a>.  I've never liked the complexity of the nested set model, though I realize for a long while it was the only route to take on some databases.  It was very interesting to me to see how much faster using a recursive common table expression on the adjacency list model is over a standard nested set model query.

After reading that post, I wanted to create a plugin that makes it very easy to use recursive common table expressions to load tree structured data, similar to how the various nested set plugins handle all of the related complexity for you.  The rcte_tree plugin isn't currently very full featured, but what it does it should do well, and it should perform well.  Considering that the adjacency list model is much simpler in terms of storage, as long as your database supports recursive common table expressions, there's really no reason to store your tree structured data using a nested set.

The plugin's implementation has a few potentially interesting parts.  First, there are no submodules, it just has a single singleton apply method that adds 4 associations to the model.  Most of the work in the plugin is setting up correct :dataset, :after_load, and :eager_loader options for the ancestor and descendant associations.  Like the standard Sequel associations code, the general strategy is to precompute most of the objects you need, store them in local variables, and access them from closures.  I didn't take an extreme approach on this, and there are a few places left where objects that could be precomputed weren't, but in general it's a fairly tight implementation.  Most of the work in the :after_load and :eager_loader options relates to populating the associations cache for all returned objects, allowing you to do the following:

    Model.plugin :rcte_tree
    m = Model.first
    m.ancestors
    m.descendants
    # No more database queries
    m.parent.parent.parent
    m.children.map do |c1|
      c1.children.map do |c2|
        c2.children.map do |c3|
          #...
        end
      end
    end

The other interesting thing in the plugin is a cool hack that uses the nested eager loading capability to specify the number of levels to which to eagerly load descendants:

    Model.filter(:id=>[1,2,3]).eager(:descendants=>2)

Usually, when using the eager method with a hash, values should be arrays, hashes, or symbols, specifying nested associations to eagerly load for all associated objects returned by this association.  However, the implementation pushes actually implementing this to inside the :eager_loader proc, which means that custom eager loaders can actually use this to make behavioral decisions while eagerly loading.  In this case, if you specify an Integer as a nested association (which is not normally valid), the descendants eager loader will recognize it and treat it as the number of levels to eagerly load.  Unfortunately, no such cool hack works for lazy loading, so if you want to only load descendants to a given level, you need to use eager loading.

Please give this plugin a shot and let me know how it works for you.
