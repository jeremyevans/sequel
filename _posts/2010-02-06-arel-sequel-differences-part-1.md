---
 layout: post
 title: Arel<->Sequel Differences (Part 1)
---

Recently, Nick Kallen, the original author of Arel, <a href="http://magicscalingsprinkles.wordpress.com/2010/01/28/why-i-wrote-arel/">posted about the reasons behind Arel's creation</a>.  I found it odd that the post never mentioned Sequel, since the building-queries-via-method-chaining style that Arel uses was used by Sequel before Nick started work on Arel.  Nick acknowledges in the comments that <a href="http://magicscalingsprinkles.wordpress.com/2010/01/28/why-i-wrote-arel/#comment-175">he was aware of Sequel when he started work on Arel</a>, but that he gives two reasons for not using Sequel (he may have more reasons, but he only lists two):

* Sequel did not support joining the same table to itself transparently
* Sequel did not support closure when joining with aggregations

This blog post will address the first point, a future blog post will address the second.

It is true that for the default case, Sequel does not automatically alias when the same table is joined to itself:

    DB[:comments].join(:comments)
    # SELECT * FROM comments INNER JOIN comments

However, if you want to join to an arbitrary dataset, Sequel does alias it for you:

    DB[:comments].join(DB[:comments])
    # SELECT * FROM comments
    #  INNER JOIN (SELECT * FROM comments) AS t1

Let's consider why one would want automatic aliasing when joining.  For the vast majority of joins, all tables being used in the query are known in advance.  And virtually all joins need join criteria that require some knowledge of both the existing dataset and the dataset being joined.  So what usage types actually benefit from automatic aliasing?  The main use case I can think of where automatic aliasing is helpful is when you have partial knowledge about the dataset being joined.  For example, if you know enough about the dataset to join to it, but not enough to know all tables in the dataset.  

The use case I've just described does happen, and actually happens whenever you eager load via joins in Sequel::Model.  Because of the way eager loading via joins works, in the model classes you specify the join conditions to get to the next table, but since you can cascade eager loading (e.g. comments->subcomments->subsubcomments), when you specify the conditions in your models, you don't know until runtime which aliases will be used.  Sequel has specific automatic aliasing logic to handle this case:

    Comment.eager_graph(:comments).all
    # SELECT ... FROM comments
    #  LEFT OUTER JOIN comments AS comments_0
    #   ON (comments_0.id = comments.id)

This logic is not built into Dataset#join, instead, it is handled by the eager loading code, since that's the only place that has needed it.

In the two years I have been the Sequel maintainer, I cannot recall a request for automatic aliasing when joining where Sequel does not already do it.  It is possible to add a similar feature to Sequel, but I haven't had any requests for one, nor have a seen a real use case that requires it.  If you'd like to share a real use case that Sequel doesn't already handle, please post in the comments.

One issue with automatic aliasing is the following type of code:

    dataset.left_join(:comments, :parent_id=>:id).
     filter({:comments__author_id=>1, :comments__id=>nil}.sql_or)

This will join the comments table to the dataset, but filter it to only include rows where the joined table's author id is 1 or where there wasn't a matching row in the joined table.  Basically, it is a situation where you cannot include the conditions in the join criteria (because it is a left join), but must use the same alias in the WHERE clause.  If Sequel used automatic aliasing, this type of code would change meaning:

    dataset = DB[:comments].select(:comments.*)
    dataset.left_join(:comments, :parent_id=>:id).
     filter({:comments__author_id=>1, :comments__id=>nil}.sql_or)
    # SELECT comments.* FROM comments
    #  LEFT JOIN comments AS comments_0
    #   ON (comments_0.parent_id = comments.id)
    # WHERE ((comments.author_id = 1) OR (comments.id IS NULL))

Instead of returning all comments that either don't have subcomments or have subcomments with an author_id of 1, this returns all comments with an author_id of 1.  This is where automatic aliasing bites you, and why I don't think the benefit of automatic aliasing is worth the cost.  This automatic aliasing problem happens in Arel:

    relation = comments = Table(:comments)
    relation.outer_join(comments) do |rel, coms|
      rel[:id].eq(coms[:parent_id])
    end.where(comments[:author_id].eq(1).
      or(comments[:id].eq(nil)))

Now, you are warned in <a href="http://github.com/rails/arel/blob/master/README.markdown">Arel's README</a> not to do this.  But while Sequel will raise an exception if you do this, Arel will silently work and just not do what you want.  The offical Arel way to handle this would be to create an alias of the relation you want to use first.  That way, you are assured that things will work.  Unfortunately, that means that every single time you don't have complete knowledge of the relations you are using, you must create the alias, because if you don't, instead of an error being raised, you'll get unexpected behavior.

If you want Arel's behavior in Sequel, it's not hard to do:

    aliaz = nil
    dataset = DB[:comments].select(:comments.*)
    dataset.left_join(DB[:comments]) do |j,lj,js|
      aliaz = j
      {:parent_id.qualify(j)=>:id.qualify(lj)}
    end.filter({:author_id.qualify(aliaz)=>1,
                :id.qualify(aliaz)=>nil}.sql_or)
    # SELECT comments.* FROM comments
    #  LEFT JOIN (SELECT * FROM comments) AS t1
    #   ON (t1.parent_id = comments.id)
    # WHERE ((t1.author_id = 1) OR (t1.id IS NULL))

Basically, you just have to make sure you are using a dataset instead of a plain symbol, copy the first argument of the block to a local variable, and reference that later in the WHERE clause.  In this case, Arel's ability to give you the alias in advance is a little nicer, so assuming someone actually asked for it, I wouldn't be opposed to something like this:

    aliaz = dataset.next_alias(:comments)
    dataset.
     left_join(:comments.as(aliaz), :parent_id=>:id).
     filter({:author_id.qualify(aliaz)=>1,
             :id.qualify(aliaz)=>nil}.sql_or)

That would make creating an available alias as easy as it is in Arel, while avoiding all of the problems that come with automatic aliasing.  Again, no one has asked for this, so I'm not sure if it is a problem worth solving.
