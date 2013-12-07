---
 layout: post
 title: 2 Long Lived Bugs Squashed
---

Earlier today I pushed quite a couple commits to github that squashed some long lived bugs in Sequel.

The first commit <a href="http://github.com/jeremyevans/sequel/commit/a787763843d9a57742cb49ac81ea32d47b109ae0">fixed the usage of eager graphing with database schemas</a>.  For those of you who aren't familar with Sequel::Model, eager graphing is the term used to describing eager loading of model associations using JOINs instead of loading each association in a separate query.  It's necessary if you want to filter or order the main and associated objects returned based on attributes in associated tables.  Database schemas are supported by many databases in order to separate or group related tables.  Sequel has good support for both eager graphing and database schemas, but the two didn't work together until now.

The reason for that the two concepts didn't work well in conjunction was that Sequel didn't have a way previously to take an object representing a schema-qualified table, and return just the table name, sans schema, which was sometimes necessary due to aliasing issues.  Part of this difficulty came from the fact that in Sequel, symbols represent SQL identifiers, and can included embedded qualifiers (i.e. ruby :schema__table => SQL schema.table), which weren't getting handled appropriately.  <a href="http://groups.google.com/group/sequel-talk/browse_thread/thread/f377335abfe4a0b5">John Firebaugh brought this issue to my attention</a>, and it took a few iterations and some help from Mike Luu in order to get the basic code so that implicitly qualified symbols representing schema-qualified tables could work well with eager graphing.

I generally don't like solutions that only fix part of the problem, so one of the reasons it took so long to get a patch committed (2 weeks, an eternity in terms of a known bug in Sequel) is that I wanted to commit something that fixed as many similar cases as possible.  I originally had a much more invasive patch where I was adding quite a few APIs while I thought about possible issues.  I ended up throwing all of that away and decided to use a test-driven development approach, by writing PostgreSQL adapter tests for the functionality (since PostgreSQL supports schemas and is one of the databases I regularly test with).  I then started adding some of the code I worked on earlier as needed to pass the tests I had written.

This ended up being a much cleaner patch, only requiring two dataset methods being added, alias_symbol and alias_alias_symbol.  Dataset#alias_symbol accepts a few possible representations of identifiers in Sequel (Symbols, Strings, SQL::Identifier, SQL::QualifiedIdentifier, and SQL::AliasedExpression), and outputs just the alias they represent in Symbol form.  Dataset#alias_alias_symbol accepts a few possible representations of aliases in Sequel (Symbols, Strings, and SQL::Identifiers), and outputs the alias represented in Symbol form.  Then, the eager graphing code was changed to use these new methods, and then all of the tests passed.  Well, not right away, as some of the tests I had written had bugs, but that got me 90% of the way there.

The second long lived bug dealt with <a href="http://github.com/jeremyevans/sequel/commit/6984690cd068ee649ccfbb41225b9f789212c689">fixing the handling of IN/NOT IN with an empty array of objects</a>, which used to be represented by like this:

    dataset.filter(:c=>[])  # c IN (NULL)
    dataset.exclude(:c=>[]) # c NOT IN (NULL)

It turns out this type of IN works OK on some databases, but this type of NOT IN definitely didn't work.  I actually stumbled upon this while browsing <a href="http://www.sqlalchemy.org/trac/wiki/FAQ">SQLAlchemy's FAQ</a>, which describes how they handle it.   I choose to use a similar approach for the IN case, with my own approach for the NOT IN case:

    dataset.filter(:c=>[])  # c != c
    dataset.exclude(:c=>[]) # 1 = 1

I chose to handle IN the same way as SQLAlchemy, since it has correct NULL handling.  The SQLAlchemy FAQ answer doesn't state how they handle NOT IN with the empty array.  The reason I choose to use an expression that would evaluate to true on all databases is that if the array is empty, then even if the value of column is NULL, it's won't be counted as contained in the array.

That was the easier part.  However, Sequel doesn't stop there, since it also supports multiple columns in IN/NOT IN.  These constructs used to be handled the same way:

    dataset.filter([:c, :c2]=>[])  # (c, c2) IN (NULL)
    dataset.exclude([:c, :c2]=>[]) # (c, c2) NOT IN (NULL)

Which was also wrong in the NOT IN case for the same reason.  Now these cases are handled like this:

    dataset.filter([:c, :c2]=>[])  # (c != c) AND (c2 != c2)
    dataset.exclude([:c, :c2]=>[]) # 1 = 1

The NOT IN is the same as the single column case for the same reason, while the IN case is a natural extension of the single column handling for multiple columns.

Now, multiple column IN/NOT IN support is not in SQL 92, and many (maybe most) databases do not support it.  However, multiple column IN/NOT IN support is necessary to handle things like eager loading associations in separate queries based on composite keys (which Sequel::Model supports).  To handle the multiple column IN/NOT IN support on databases that don't support it natively, Sequel emulates support via OR and AND:

    dataset.filter([:c, :c2]=>[[1, 2], [3, 4]].sql_array)
    # ((c = 1) AND (c2 = 2)) OR ((c = 3) AND (c2 = 4))
    dataset.exclude([:c, :c2]=>[[1, 2], [3, 4]].sql_array)
    # ((c != 1) OR (c2 != 2)) AND ((c != 3) OR (c2 != 4))

Just a quick note that the .sql_array is necessary for arrays of two element arrays, as otherwise arrays of two element arrays are treated as condition specifiers. Sequel treats such arrays like hashes, but where duplicate keys are allowed.  Anyway, as long as the array provided wasn't empty, this worked fine.  However, when the array was empty, Sequel raised a Sequel::Error in both cases.  That behavior needed to be fixed.  So Sequel's new behavior is to have empty arrays treated exactly the same no matter if the database supports multiple column IN/NOT IN or not.

One final issue related to multiple column IN/NOT IN when the database didn't support it had to do with the case when a dataset was used instead of an array:

    dataset.filter([:c, :c2]=>ds.select(:c, :c2))

Previously, Sequel didn't handle this case, and it ended up raising a TypeError.  Sequel now correctly handles the case by running the dataset passed as it's own query, getting the results, and handling it like an array:

    dataset.filter([:c, :c2]=>ds.select(:c, :c2))
    # First query: SELECT c, c2 FROM ...
    # 0 entries returned WHERE: (c != c) AND (c2 != c2) 
    # 2 entries returned WHERE: 
    #  ((c = 10) AND (c2 = 20)) OR ((c = 30) AND (c2 = 40))

This IN/NOT IN handling also is representative of how important it is to be able to represent SQL or relational concepts abstractly, as Sequel does.  If Sequel did not have an abstract concept for IN/NOT IN, and you had to represent it like:

    dataset.filter("c NOT IN ?", array)

You would not be able to handle cases where the array was empty, without using a conditional in your own code (which few people would think to do).

Fixing the IN/NOT IN with empty array issue also shows the benefit you can get by getting familiar with other similar or competing projects.  Technological progress is not a zero sum game, and taking good ideas from other projects is something that should be encouraged.
