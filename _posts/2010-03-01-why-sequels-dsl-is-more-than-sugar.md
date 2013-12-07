---
 layout: post
 title: Why Sequel's DSL Is More Than Sugar
---

In a previous post, I discussed <a href="/2010/02/07/sequel-unfriendly-and-ugly.html">whether Sequel's DSL is unfriendly and ugly</a>.  To sum up my thoughts, for plently of common things, Sequel's DSL is friendly and pretty, and for less common and more complex things, it may not be friendly and pretty, but it's still better than dropping down to SQL and dealing with placeholders (though Sequel supports that as well).

That post was in response to a comment by Pratik Naik about Sequel being unfriendly and ugly, but beauty is not the main reason for or benefit of Sequel's DSL.  The main advantages of Sequel's DSL are:

* It is generally simpler than writing the SQL query itself
* It can handle problematic SQL constructs
* It makes database independence easier

While Sequel's method chaining API has made it far more friendly that ActiveRecord's pre-version-3 API, I still think Sequel retains a strong advantage in terms of usability over ActiveRecord because of it's DSL, especially considering that the Arel DSL is not part of ActiveRecord.

# Simplicity

I think Sequel's DSL allows for simpler queries, and has a more consitent interface than SQL:

    ds = DB[:albums]
    # SELECT * FROM albums
    ds.where(:id=>nil)
    # SELECT * FROM albums WHERE (id IS NULL)
    ds.where(:id=>1)
    # SELECT * FROM albums WHERE (id = 1)
    ds.where(:id=>[1, 2, 3])
    # SELECT * FROM albums WHERE (id IN (1, 2, 3))
    ds.where(:id=>1..5)
    # SELECT * FROM albums
    # WHERE ((id >= 1) AND (id <= 5))

To be fair, ActiveRecord can do all of that for the WHERE clause (find :conditions option), but while ActiveRecord restricts the functionality to the WHERE clause, with Sequel that DSL is available almost everywhere.  For example, let's say you want the names of all albums ordered by name, but want the albums with artist_id 1 to show up before all other albums:

    ds.select(:name).reverse_order({:artist_id=>1}, :name.desc)
    # SELECT name FROM albums
    # ORDER BY (artist_id = 1) DESC, name ASC

Or maybe you want albums of a number of different artists to show up before all other artists:

    ds.select(:name).reverse_order({:artist_id=>[1, 2, 3]}, :name.desc)
    # SELECT name FROM albums
    # ORDER BY (artist_id IN (1, 2, 3)) DESC, name ASC

Note how the same hash syntax that filter uses can be used in order.  This is true pretty much everywhere in Sequel, due to this fact:

    DB.literal(:artist_id=>[1, 2, 3]})
    # => "(artist_id IN (1, 2, 3))"

With Sequel, you use ruby objects to represent all of your SQL concepts, and they stay objects until it comes time to generate the SQL, at which time they are literalized.  Sequel supports most common SQL concepts directly in ruby:

    # Casting
    ds.select(:copies_sold.cast(String))
    # SELECT CAST(copies_sold AS varchar(255)) FROM albums

    # Case Statements
    ds.select({3=>1}.case(0, :artist_id))
    # SELECT (CASE artist_id WHEN 3 THEN 1 ELSE 0 END) FROM albums

    # LIKE
    ds.select({:name.like('Pink%')=>1}.case(0))
    # SELECT (CASE WHEN (name LIKE 'Pink%') THEN 1 ELSE 0 END) FROM albums

# Problematic SQL constructs

Let's say that given an array of artist_ids, you want to return all albums that don't have one of those artist ids (basically, excluding those artists' albums).  Here's how you would do so in ActiveRecord and Sequel:

    # ActiveRecord
    class Album < ActiveRecord::Base
      def self.without_artists(bad_artists)
        find(:all, :conditions=>["artist_id NOT IN (?)", bad_artists])
      end
    end

    # Sequel
    class Album < Sequel::Model
      def self.without_artists(bad_artists)
        exclude(:artist_id=>bad_artists).all
      end
    end

There's two major differences between these approaches.  The first is that Sequel's approach will work not just for arrays, but also for integers, ranges, and datasets/subselects.  That may not be needed, but if you do need it, you can see how Sequel's DSL allows a more generic approach.

That's not the real issue here.  The real issue is that the ActiveRecord approach cannot handle the empty array properly, but Sequel can (<a href="http://github.com/jeremyevans/sequel/commit/6984690cd068ee649ccfbb41225b9f789212c689">due to very recent changes</a>).  Given the empty array, ActiveRecord will return no albums, while Sequel will return all albums.  The only way to handle this correctly with ActiveRecord is to add a conditional.  So in addition to being more generic, the Sequel code is also more robust (as well as shorter).

# Database independence

If you have to use SQL directly for all but the most trivial applications, you are going to run into database dependence issues.  If you want to target more than one database, you'll have to use conditionals (or polymorphism) somewhere to handle multiple databases.  However, Sequel provides you the ability to write fairly advanced queries in a database independent way.

## String concatenation

Even something as simple as string concatenation is handled differently on different databases.  MySQL and Microsoft SQL Server handle it differently than the SQL standard, which most other databases follow.  In Sequel, the same code works on all databases.  Let's say you want to return all names of albums in the format "Artist Name - Album Name":

    DB[:albums].
      join(:artists, :id=>:artist_id).
      select_map(:artists__name.sql_string + ' - ' + :albums__name)
      # or select_map([:artists__name, :albums__name].
      #      sql_string_join(' - '))

## Case sensitive/insensitive LIKE

Let's say you want to use a case insensitive LIKE construct.  On most databases, LIKE is case insensitive, but it isn't on PostgreSQL, which uses ILIKE for case insentive LIKE.  Sequel chooses to use PostgreSQL's name for this construct, so to do a case insensitive like in Sequel:

    DB[:albums].filter(:name.ilike('fbts'))

If you want a case sensitive LIKE, most databases don't provide one, but MySQL and PostgreSQL do, using different syntax.  With Sequel, the following will work for both:

    DB[:albums].filter(:name.like('FBtS'))

MySQL and PostgreSQL also support regular expressions, and Sequel allows the same syntax to work on both:

    # Case insensitive regexp
    DB[:albums].filter(:name => /FBtS/i)
    # Case sensitive regexp
    DB[:albums].filter(:name => /FBtS/)

## Full Text Search

If you want database independent full text searching, Sequel can help you out there too on MySQL, PostgreSQL, and MSSQL:

    DB[:albums].full_text_search(:name, 'FBtS')

## Multiple Column IN/NOT IN

Let's say you want to return all rows of a table where two of the columns both match one of a set of values in a provided array.  For example, if you are eagerly loading a model association based on composite keys, where you want all rows with [id1, id2] = [1, 2] OR [3, 4]  OR ....  With Sequel, this can be done fairly easily:

    DB[:albums].filter([:id1, :id2]=>[[1, 2], [3, 4]].sql_array)

That's not so much the interesting part.  The interesting part is that most databases cannot handle that kind of construct directly using IN/NOT IN.  Most databases can only handle a single column in IN/NOT IN.  So on most databases, Sequel emulates support:

    DB[:albums].filter([:id1, :id2]=>[[1, 2], [3, 4]].sql_array)
    # Supported: (id1, id2) IN ((1, 2), (3, 4))
    # Emulated: (id1 = 1 AND id2 = 2) OR (id1 = 3 AND id2 = 4)
    DB[:albums].exclude([:id1, :id2]=>[[1, 2], [3, 4]].sql_array)
    # Supported: (id1, id2) NOT IN ((1, 2), (3, 4))
    # Emulated: (id1 != 1 OR id2 != 2) AND (id1 != 3 OR id2 != 4)

All of these database independent things can only be done because Sequel represents SQL concepts as objects and not as literal SQL strings.
