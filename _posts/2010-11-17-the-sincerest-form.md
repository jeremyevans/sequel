---
 layout: post
 title: The Sincerest Form
---

ZOMG, a Blog II post that isn't about a new release of Sequel!

You may remember a few months ago where I talked about <a href="/2010/05/29/fun-with-graphviz-and-associations.html">visualizing association relationships between models using sequel_dot</a>.  I recently attended RubyConf and saw <a href="https://github.com/tenderlove">Aaron Patterson</a>'s <a href="http://www.slideshare.net/tenderlove/zomg-why-is-this-code-so-slow">awesome presentation</a>.   One of the things Aaron showed off in his presentation (slide 257), was a Graphviz visualization of an ARel Relation's abstract syntax tree.  I decided to produce something similar for a Sequel dataset.  So today I pushed a commit that <a href="https://github.com/jeremyevans/sequel/blob/master/lib/sequel/extensions/to_dot.rb">adds a to_dot extension to Sequel</a>.

Here are a couple of examples of output.  First, a simple example:

    DB[:items].filter(:a=>1)

<img src="/images/to_dot_simple.gif" />

Second, a more complicated example (click to enlarge):

    DB[:items, :items2].distinct.
      filter(:a=>1, :b=>[:a, :b.identifier, :c.qualify(:a)]).
      order(:a.desc, :b.asc(:nulls=>:first), :c).
      select(:a.cast(Integer).as(:b),
        {:d=>:b, :g=>:h}.case(:e, :f.sql_function(1, :b.sql_subscript(:c)))).
      select_more{sum(:over, :args=>[:a, 'a IN :v'.lit(:v=>[true, false, nil]), 
        '? = ?'.lit('c'.lit, 'd')], :partition=>[:b], :order=>:c.desc){\}\}.
      with(:a, db[:a]).
      natural_join(:b).
      join(:c, [:d]).
      join(:e, :f=>:g).
      group(:d, :e).
      having{e < g}.
      limit(42, 24).
      for_update

<a href="/images/to_dot_complex.gif"><img src="/images/to_dot_complex.gif" width="614" height="184" /></a>
