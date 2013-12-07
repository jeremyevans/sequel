---
 layout: post
 title: Lists and Trees
---

Previously, Sequel didn't have built in support for storing data structures such as lists and trees in an database.  I committed the <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/rcte_tree.rb">rcte_tree plugin</a> a few versions back, but that's only usable on database's that support recursive common table expressions.  I determined that Sequel would benefit from having built in plugins for lists and trees, but instead of doing what I usually do (build it myself), I decided to base my work on a couple of existing external plugins.

For the new <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/list.rb">list plugin</a>, I based my work on <a href="http://github.com/aemadrid/sequel_orderable">Adrian Madrid's sequel_orderable plugin</a>, which was itself based on an a version by Aman Gupta.  For the new <a href="http://github.com/jeremyevans/sequel/blob/master/lib/sequel/plugins/tree.rb">tree plugin</a>, I based my work on <a href="http://github.com/mwlang/sequel_plus/blob/master/lib/sequel_tree.rb">Michael Lang's sequel_tree plugin</a>.  These plugins are the first built-in ones that were not either extracted from Sequel or written originally by me.  Like many programmers, I tend to have a slight "not invented here" mindset, but both of these plugins came with good integration tests, and I did write my own set of mocked specs, so I feel comfortable shipping them with Sequel.

Both plugins are fairly simple to use.  The list plugin allows you to easily get the next and previous entries in the list:

    Item.plugin :list
    item = Item[1]
    item.next
    item.prev

You can also modify the item's position, which involves shifting the position of other items in the list:

    item.move_to(3)
    item.move_to_top
    item.move_to_bottom
    item.move_up
    item.move_down

For the list plugin, the only thing to keep in mind is that the first position is 1, not 0.  It's possible that an option for 0-based lists will be added if such a feature is requested by the community.

The tree plugin allows you to easily get the parent and children of a given node:

    Node.plugin :tree
    node = Node[1]
    node.parent
    node.children

You can always get all ancestors and descendants of a given node, though this takes one query per tree level:

    node.ancestors
    node.descendants

It's also easy to get siblings of a node, which are the children of a node's parent, excepting the node itself:

    node.siblings

Finally, you can get the root for a specific note, or an array of all root nodes in the table:

    node.root
    Node.roots

As you may expect from Sequel, there are additional options, so please review the RDoc and/or specs if you are interested in those.
