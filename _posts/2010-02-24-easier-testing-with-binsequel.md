---
 layout: post
 title: Easier Testing With bin/sequel
---

One of the commits I pushed to github a few days ago <a href="http://github.com/jeremyevans/sequel/commit/83baf16a221ac078c780f1d885f0c698dd7b460f">adds support for using bin/sequel without a database argument</a>. So with a current checkout, you can do:

    $ ruby -I lib bin/sequel
    Your database is stored in DB...
    irb(main):001:0>

First, why would you want to do this?  Well, in most cases, you wouldn't, as very little works:

    irb(main):001:0> DB[:t].all
    NotImplementedError: This method must be overridden in Sequel adapters
        from ./lib/sequel/dataset/actions.rb:69:in `fetch_rows'
        from ./lib/sequel/dataset/actions.rb:61:in `each'
        from ./lib/sequel/dataset/actions.rb:13:in `all'
        from (irb):1
    irb(main):002:0> DB[:t].insert(:c=>1)
    NotImplementedError: #execute should be overridden by adapters
        from ./lib/sequel/database.rb:326:in `execute'
        from ./lib/sequel/database.rb:340:in `execute_dui'
        from ./lib/sequel/database.rb:347:in `execute_insert'
        from ./lib/sequel/dataset/actions.rb:116:in `execute_insert'
        from ./lib/sequel/dataset/actions.rb:76:in `insert'
        from (irb):2
    irb(main):003:0> DB[:t].update(:c=>1)
    NotImplementedError: #execute should be overridden by adapters
        from ./lib/sequel/database.rb:326:in `execute'
        from ./lib/sequel/database.rb:340:in `execute_dui'
        from ./lib/sequel/dataset/actions.rb:111:in `execute_dui'
        from ./lib/sequel/dataset/actions.rb:93:in `update'
        from (irb):3
    irb(main):004:0> DB[:t].delete
    NotImplementedError: #execute should be overridden by adapters
        from ./lib/sequel/database.rb:326:in `execute'
        from ./lib/sequel/database.rb:340:in `execute_dui'
        from ./lib/sequel/dataset/actions.rb:111:in `execute_dui'
        from ./lib/sequel/dataset/actions.rb:46:in `delete'
        from (irb):4
    irb(main):005:0> DB.create_table(:a){primary_key :id}
    NotImplementedError: #execute should be overridden by adapters
        from ./lib/sequel/database.rb:326:in `execute'
        from ./lib/sequel/database.rb:340:in `execute_dui'
        from ./lib/sequel/database.rb:333:in `execute_ddl'
        from ./lib/sequel/database/schema_methods.rb:188:in `create_table_from_generator'
        from ./lib/sequel/database/schema_methods.rb:73:in `create_table'
        from (irb):5

So what's the point?  Well, as the commit message indicates, it's very helpful for quick testing, or for learning how to use the library:

    irb(main):006:0> puts DB[:t].filter{num > 6}.exclude(:id=>DB[:c].select(:t_id)).sql
    SELECT * FROM t WHERE ((num > 6) AND (id NOT IN (SELECT t_id FROM c)))
    => nil
    irb(main):007:0> puts DB[:t].join(:c, :t_id=>:id).join(:d, :c_id=>:id).sql
    SELECT * FROM t INNER JOIN c ON (c.t_id = t.id) INNER JOIN d ON (d.c_id = c.id)
    => nil

Previously, whenever I wanted to do a quick test to see if Sequel is generating the correct SQL, I'd use the "sqlite:/" as the database argument, but the SQL produced isn't as friendly:

   irb(main):001:0> puts DB[:t].join(:c, :t_id=>:id).sql
   SELECT * FROM `t` INNER JOIN `c` ON (`c`.`t_id` = `t`.`id`)
   => nil

So the main purpose for this commit is really to make my life easier, but I think it's helpful for just playing around, especially if you don't have SQLite installed and don't want to experiment while connected to a real database.
