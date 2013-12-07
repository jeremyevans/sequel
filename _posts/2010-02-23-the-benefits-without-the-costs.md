---
 layout: post
 title: The Benefits Without the Costs
---

In a <a href="/2010/02/06/arel-sequel-differences-part-1.html">previous post discussing Sequel and Arel</a>, I mentioned that I think Arel's choice to automatically alias relations leads to unexpected behavior, where Sequel would raise an error instead.  However, I did admit that Arel made it much easier for the user to get a previously unused alias that could be used manually to ensure correct behavior.

Previously, the Sequel code that created unused table aliases was a private method only used by the eager graphing code, and it wasn't exposed to the user.  However, <a href="http://github.com/jeremyevans/sequel/commit/bfd4f2d145d8e50be557c884786f92484b0f38c3">in a recent commit I renamed the method and made it public</a>.  This makes using Sequel as easy as Arel for when you want to join a table to a dataset/relation, while being sure that it doesn't conflict with an existing table in the dataset/relation:

    # Arel, taken from the README:
    comments = Table(:comments)
    replies = comments.alias
    comments_with_replies = \
      comments.
       join(replies).
       on(replies[:parent_id].eq(comments[:id]))
    # SQL: SELECT * FROM comments
    #      INNER JOIN comments AS comments_2
    #      WHERE comments_2.parent_id = comments.id
    
    # Sequel:
    comments = DB[:comments]
    replies = comments.unused_table_alias(:comments)
    comments_with_replies = \
      comments.
       join(:comments.as(replies), :parent_id=>:id)
    # SQL: SELECT * FROM comments
    #      INNER JOIN comments AS comments_0
    #       ON (comments_0.parent_id = comments.id)

Note that the resulting SQL is different.  Arel uses a WHERE clause for the join conditions (even though the conditions were added with .on), while Sequel adds the conditions to ON clause of the JOIN statement.  Because this is an INNER JOIN, either way is acceptable.  I'm not sure how Arel would handle LEFT JOINs, since adding the conditions to the WHERE clause in that case would not ensure the same results (if you know how Arel handles it, please post in the comments).  Other than that minor issue and the fact that the aliases chosen are different, the SQL produced is equivalent.

Let's now move to the ruby code.  Right off the bat, we see that creating an alias is different, but the way it is fundementally different is not obvious here.  With Arel, the alias is generated from the relation that you want to join to the existing relation.  With Sequel, the alias is generated from the existing dataset itself.  This is a hard distinction to see from the code given, since in this case the existing dataset/relation and the dataset/relation that you are joining to it are the same.  The underlying reason is that Arel delays the alias creation until the SQL query is produced (I assume), whereas in Sequel the alias is set when the table is joined to the dataset.

Because of this difference, you don't need to provide an argument when producing in alias in Arel, since it knows what table you want, while you do in Sequel, since you could supply any symbol to it.  Note that Sequel just returns an unused alias, it doesn't care how it is applied.  So a Sequel user would probably do the following instead, since it leads to more obvious code:

    # Sequel:
    comments = DB[:comments]
    replies = comments.unused_table_alias(:replies)
    comments_with_replies = \
      comments.
       join(:comments.as(replies), :parent_id=>:id)
    # SQL: SELECT * FROM comments
    #      INNER JOIN comments AS replies 
    #       ON replies.parent_id = comments.id)

In Sequel, the argument to unused_table_alias doesn't need to be related in any way to an existing table name, so it's best to pick something semantically meaningful.

The second significant difference between Arel and Sequel in this regard is that in Arel, the alias method returns a relation, whereas in Sequel, the unused_table_alias method returns a plain symbol that you can use as an alias.  This is why in Arel, you can directly join to the object alias returns, whereas in Sequel, you need to take the table you want to use and alias it to the symbol returned by unused_table_alias before joining.

One minor difference between Sequel and Arel in this example is that you need to explicitly qualify the identifiers with Arel, whereas Sequel will take the conditions hash and handle the qualifying implicitly (with keys being qualified to the argument to join, and values to the last joined or initial table).

Honestly, Arel has a slightly nicer API for this particular use case, since it appears to be one of the things Arel was specifically designed to handle.  Sequel's API is functionally equivalent, and still easy to use, and considering how often this method is needed (almost never by user code), I think it more than satisfies any potential need.
