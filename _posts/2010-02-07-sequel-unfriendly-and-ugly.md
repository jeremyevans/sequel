---
 layout: post
 title: "Sequel: Unfriendly and Ugly?"
---

Recently, in comments to a <a href="http://m.onkey.org/2010/1/22/active-record-query-interface">post describing ActiveRecord's 3.0 new Arel-based API</a>, Pratik Naik had the following to say:

> 2) I think we should wait before making DSL for every sql thing.
> Strings work just fine in many cases. I'd hate to end up with a
> DSL like sequel, which is very unfriendly and rather ugly for
> someone looking at it for the first time.

First, I'd like to say that I think it's a good idea to wait and give a lot of thought before adding methods/DSL for a given SQL syntax.  There are a few places where I added methods to Sequel that in hindsight probably are not used often enough to justify a method. For example, the methods for creating functions/triggers in the PostgreSQL adapter.  There's also places where Sequel doesn't currently provide methods, where they might be added later. For example, Sequel currently doesn't have methods/options to handle transaction isolation levels, or methods to handle granting/revoking permissions.

I also agree that "strings work just fine in many cases".  Basically, if you are only targetting a single database, and have no plans to ever support another database, or you know that the SQL syntax you are using works on all databases you ever plan to support, using a literal SQL string is fine.

I don't think that Sequel's DSL is "very unfriendly", or even a little unfriendly.  Also, I can't recall any first time users saying that thought the DSL was ugly, though I do admit that it's unlikely I'd hear from them in that case.

For those of you who are unfamiliar with Sequel, I'd like to present some examples of ActiveRecord/Arel and Sequel code, so you can judge for yourself:

    # Arel                           # Sequel
    users = Table(:users)            users = DB[:users]                                                              
    users.where(users[:name].eq(1))  users.where(:name=>1)
    users.project(users[:id])        users.select(:id)
    users.where(users[:age].lt(25))  users.where{age < 25}
    
    ## Execute arbitrary SQL
    # ActiveRecord
    ActiveRecord::Base.connection.execute 'SQL'
    # Sequel
    DB.run 'SQL'

    ## Get array of names for all people
    # ActiveRecord
    Person.select('name').all.map(&:name)
    # Sequel
    Person.select_map(:name)

OK, maybe that's not really fair.  Let's try a more advanced usage, comparing Sequel's DSL to places where you have to use strings in ActiveRecord:

    # ActiveRecord
    Person.where('country_id = ? AND (name NOT LIKE ? OR age < ?)', 1, 'Jill%', 15)
    Person.where('country_id = :country_id AND (name NOT LIKE :pattern OR age < :min_age)',
     :country_id=>1, :pattern=>'Jill%', :min_age=>15)
    # Sequel
    Person.where{ {country_id=>1} & (~name.like('Jill%') | (age < 15))}

Is that unfriendly or ugly? Maybe for some people, it is.  Some people might think the overriding of the bitwise operators (&, |, and ~) ugly, especially considering precedence rules require parentheses around other statements.  Personally, I find it uglier to have to use a string with placeholders, either with question marks or with named placeholders.  The problem with question mark placeholders is that you have to remember the order in which to play the arguments.  This is not too difficult when there are only a few arguments, but for complex cases with many arguments, it is a real problem.  The problem with the hash syntax is that you have to duplicate the keys of the hash inside the string, so the code is both redundant and more verbose.

Using Sequel's DSL syntax, the arguments are used directly in the query, resulting in more concise and less redundant code.  If you are familiar with <a href="http://mwrc2009.confreaks.com/14-mar-2009-18-10-the-building-blocks-of-modularity-jim-weirich.html">Jim Weirich's presentations on connascence</a>, Sequel's DSL eliminates the connascence required by the string interpolation, connascence of position in the question mark case and connascence of name in the named placeholder case.

One important thing you should note there is that both of those string-with-placeholder calls are valid Sequel code as well as valid ActiveRecord code.  So it's not like Sequel forces you to use the DSL, it's just available as an option should you want to use it.

If you are interested in more Sequel DSL examples, check out <a href="http://sequel.jeremyevans.net/rdoc/files/doc/cheat_sheet_rdoc.html">Sequel's cheat sheet</a>.

I'm interested in other's thoughts on this, so please post in the comments if you've tried Sequel, and let me know whether you think Sequel's DSL is unfriendly, and what your first impressions of Sequel's DSL were.
