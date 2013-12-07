---
 layout: post
 title: Arel<->Sequel Differences (Part 2)
---

This is a continuation of the the series on differences between Sequel and Arel.  In <a href="/2010/02/06/arel-sequel-differences-part-1.html">the last post</a>, I described how Sequel handles aliasing when joining, and why I think the Arel approach of automatic aliasing is problematic.  In this post, I'm going to address <a href="http://magicscalingsprinkles.wordpress.com/2010/01/28/why-i-wrote-arel/#comment-175">Nick Kallen's second reason for not using Sequel</a>, which is that he thought Sequel did not support closure when joining with aggretations.

For those of you who aren't sure what "closure when joining with aggretations" is, you should read <a href="http://magicscalingsprinkles.wordpress.com/2010/01/28/why-i-wrote-arel/">Nick's post</a>.  The example he gives is a situation where have a one-to-many relationship from one table to another and you want to select everything from the one table along with a count of matching rows in the many table.  Here's the SQL code he wants to produce:

    SELECT users.*, photos_aggregation.cnt
    FROM users
    LEFT OUTER JOIN (
      SELECT user_id, count(*) as cnt
      FROM photos
      GROUP BY user_id
     ) AS photos_aggregation
    ON photos_aggregation.user_id = users.id

Here is the Arel code you need to produce that:

    photos = Table(:photos)
    users = Table(:users)
    photo_counts = photos.
     group(photos[:user_id]).
     project(photos[:user_id], photos[:id].count)
    users.
     join(photo_counts).
     on(users[:id].eq(photo_counts[:user_id]))

Here is the Sequel code to produce an equivalent result:

    DB[:users].
     left_join(DB[:photos].
      group_and_count(:user_id.as(:id)), [:id])

I won't belabour the obvious fact that the Sequel code is much shorter, as that isn't the main point here.  I should mention that the SQL Sequel produces here isn't identical to the SQL Arel produces:

    SELECT * FROM users
    LEFT JOIN (
      SELECT user_id AS id, count(*) AS count
      FROM photos
      GROUP BY user_id
      ORDER BY count
     ) AS t1
    USING (id)

First, what's the difference between Sequel's SQL and Arel's SQL?  Well, for one, Arel is automatically assuming what you want to select in the outer query.  I haven't looked at Arel's internals, but my guess is it is taking everything from users.* because users is the first table, and then it is taking all fields from the joined relation (photo_counts) that aren't part of of the JOIN criteria.  In this case, the selection is the same as in Sequel, as Sequel uses JOIN USING instead of JOIN ON, so the id field will only appear once in the output.  Another difference is that Sequel orders the subselect, which is just a consequence of using the group_and_count helper method.  The alias used for the subselect is different, and Sequel uses LEFT JOIN instead of the more verbose LEFT OUTER JOIN. Other than those 4 minor things that don't affect the result, there is no difference.

Now let's break down the code differences between the two, starting with the subselect:

    # Arel
    photo_counts = photos.
     group(photos[:user_id]).
     project(photos[:user_id], photos[:id].count)
    # Sequel
    DB[:photos].group_and_count(:user_id.as(:id))

There really isn't much to say about this part.  Sequel has a built-in helper method for grouping and counting, since it is common. Otherwise, the code is very similar.  Now for the join:

    # Arel
    users.
     join(XXX).
     on(users[:id].eq(photo_counts[:user_id]))
    # Sequel
    DB[:users].left_join(XXX, [:id])

Here there are a couple important differences.  For one, Arel automatically uses a LEFT OUTER JOIN instead of an INNER JOIN when you use the join method.  This is different than Sequel, where Dataset#join will do an inner join.  If you want an inner join, because maybe you only care about users that have at least one picture, I'm not sure how to do so in Arel (if you know, please post in the comments).

The second important thing to note is that Arel separates the join criteria from the join method call itself, while Sequel includes that criteria in the join method call ([:id] meaning "USING (id)").  I think Sequel's API is superior here, because I consider join criteria to be part of the join itself, but maybe having relation.join do a cartesian product and making the .on similar to .where was intentional for pure relational algebra reasons.

Now that we've covered both the code and SQL syntax differences, let's think about the similarities.  If you read <a href="http://magicscalingsprinkles.wordpress.com/2010/01/28/why-i-wrote-arel/">Nick's post</a>, you'll see that he says that he has never seen anyone get the SQL query correct in an interview.  However, in order for you to write the correct Arel code, you need to know that you are doing a join to a grouped relation, which is exactly the same as what you would need to know to write the correct Sequel code.

I wonder if you are thinking what I'm thinking, which is that this idea of "closure when joining with aggretations" isn't any different between Sequel and Arel, it's just that Sequel doesn't make a big deal out of it, mostly because it's taken for granted.

Note that Sequel was capable of the same SQL query before I took over maintenance of Sequel.  Support for joining to datasets was added to Sequel on February 15, 2008.  The syntax was slightly different, but pretty much the same (this will also work on the current Sequel version):

    DB[:users].
     left_outer_join(DB[:photos].
      group_and_count(:user_id), :user_id=>:id)

Maybe Nick started work on Arel before that, but even if so, it was only a seven line patch to Sequel to support joining datasets (see commit <a href="http://github.com/jeremyevans/sequel/commit/1e242744ea1869f402e2aee2189f209b58c2598c">1e242744ea1869f402e2aee2189f209b58c2598c</a>).  I'm not saying Nick shouldn't have started Arel, but I'll go on record as saying that I haven't see how Arel is an improvement in any way over Sequel for accessing an SQL database.  I've heard talk of Arel working with non-SQL databases, and if true, that's certainly something that Sequel doesn't do (and I have no interest in doing).
