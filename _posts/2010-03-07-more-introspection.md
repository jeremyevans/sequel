---
 layout: post
 title: More introspection
---

I recently added <a href="http://github.com/jeremyevans/sequel/commit/46bd13680ab9ff78978ee57425582512e258d6e5">a patch to Sequel that adds a Dataset#first_source_table method</a>.  first_source_table is similar in many ways to the existing first_source_alias method, the only significant difference is that if the first source of the dataset contains an aliased expression (either an SQL::AliasedExpression or a implicitly aliased symbol such as :table___alias), first_source_table will return the table name, while first_source_alias will return the alias name.

first_source_alias is the method that is more commonly needed, which is why it has been around a lot longer.  For example, let's say you have albums and artists tables, and you want a dataset method that can be used for either an albums or artists dataset that will filter the dataset to just those records matching a given LIKE pattern:

    class Sequel::Dataset
      def with_name_like(pattern)
        filter(:name.like(pattern))
      end
    end

That works in the simple case, but what about the following code:

    DB[:albums].
     with_name_like('J%').
     join(:artists, :id=>:artist_id)
    # SELECT * FROM albums
    # INNER JOIN artists
    #  ON (artists.id = albums.artist_id)
    # WHERE (name LIKE 'J%')

If both the albums and artists tables have columns named name, that code will raise a database error because the name reference in the WHERE clause is ambiguous.  So a better way to write this would be:

    class Sequel::Dataset
      def with_name_like(pattern)
        filter(:name.qualify(first_source_alias).like(pattern))
      end
    end

That way, both of the following method chains work correctly:

    DB[:albums].
     with_name_like('J%').
     join(:artists, :id=>:artist_id)
    # SELECT * FROM albums
    # INNER JOIN artists
    #  ON (artists.id = albums.artist_id)
    # WHERE (albums.name LIKE 'J%')
    
    DB[:artists].
     with_name_like('J%').
     join(:albums, :artist_id=>:id)
    # SELECT * FROM artists
    # INNER JOIN albums
    #  ON (albums.artist_id = artists.id)
    # WHERE (artists.name LIKE 'J%')

So that's one of the use cases for first_source_alias.  How about a use case for first_source_table?  The main use case I can think of, and the main reason I thought to add first_source_table, is when you need to join a table back to itself, such as the situation described in <a href="http://groups.google.com/group/sequel-talk/browse_thread/thread/463899327058fdf5">a recent mailing list post by kdf</a>.  kdf's situation was where the table stores versioned data by date, and you want to get the value of a latest record.  The code I gave kdf when he asked for something that generates the necessary filters automatically was:

    class Sequel::Dataset
      # Latest by effective date
      def lbed
        s = first_source
        a = unused_table_alias(s)
        filter(:effdt.qualify(s)=>DB[s.as(a)].
         select{max(:effdt.qualify(a))}.
         filter(:fieldname.qualify(s)=>:fieldname.qualify(a),
          :fieldvalue.qualify(s)=>:fieldvalue.qualify(a))) 
      end
    end

This code does work, unless the first source of the dataset is an aliased expression, in which case it uses the aliased name instead of the actual name:

    DB[:t___a].lbed
    # SELECT * FROM t AS a
    # WHERE (a.effdt IN (
    #  SELECT max(a_0.effdt)
    #  FROM a AS a_0 -- <- Problem here
    #  WHERE ((a.fieldname = a_0.fieldname)
    #   AND (a.fieldvalue = a_0.fieldvalue))))

With first_source_table, this can be handled correctly:

    class Sequel::Dataset
      # Latest by effective date
      def lbed
        t = first_source_table
        s = first_source_alias
        a = unused_table_alias(t)
        filter(:effdt.qualify(s)=>DB[t.as(a)].
         select{max(:effdt.qualify(a))}.
         filter(:fieldname.qualify(s)=>:fieldname.qualify(a),
          :fieldvalue.qualify(s)=>:fieldvalue.qualify(a))) 
      end
    end
    DB[:t___a].lbed
    # SELECT * FROM t AS a
    # WHERE (a.effdt IN (
    #  SELECT max(t.effdt)
    #  FROM t AS t -- <- No problem here
    #  WHERE ((a.fieldname = t.fieldname) 
    #   AND (a.fieldvalue = t.fieldvalue))))

Note that you do have to use both first_source_table and first_source_alias.  first_source_table is used to get the actual table, which we create an alias to in order to use in the subquery unambiguously.  The first_source_alias method is used to reference the columns from the original table in the subquery (a correlated subquery).

Hopefully that helps shed some light on the differences between these two methods, and how to use them.
