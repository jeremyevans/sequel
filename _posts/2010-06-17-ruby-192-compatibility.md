---
 layout: post
 title: Ruby 1.9.2 Compatibility
---

I recently added a few patches that make Sequel compatible with 1.9.2.  While the jump from 1.9.1 to 1.9.2 is not nearly as significant as the jump from 1.8.x to 1.9.1, there are definitely a few issues that you should be aware of.  This post will describe the issues that I ran into when adding 1.9.2 compatibility to Sequel, and the fixes I used.

### Load path changes

Even before I upgraded, I knew that the load path changes in 1.9.2 were going to affect the specs, since <a href="http://groups.google.com/group/sequel-talk/msg/da9fdf1e96952dd1">Nate Wiger pointed it out to me on sequel-talk</a>.  In code, here is the difference:

    # 1.9.1
    $:.include?('.') # => true

    # 1.9.2
    $:.include?('.') # => false

The only part of Sequel that this change affected was the specs, and it was solved with <a href="http://github.com/jeremyevans/sequel/commit/36eabe31d10f0e9ee71ce1063852939807f446ea">a mostly mechanical change to use File.expand_path</a> to make sure that all require's in the specs used absolute paths.

The reason for this change is supposedly security, and while I can certainly understand the potential for security problems in some cases, having '.' in the load path was a convenience I use in a lot of my applications.  Let's face it, if you are passing arbitrary strings to require, you are likely to have security issues anyway.

### inspect difference

This was due to a bug in Sequel, which previously was not exposed.  However, due to a <a href="http://redmine.ruby-lang.org/issues/show/1786">fairly obscure change in ruby involving inspect</a>, the bug started started occuring.  The simplest code that I can think of that exposes this:

    # 1.9.1
    Class.new{def inspect; @a=1; super; end; def to_s(a); 'a'; end}.new.inspect
    # => #<#<Class:0xbd9080>:0xbd8f40 @a=1>

    # 1.9.2
    Class.new{def inspect; @a=1; super; end; def to_s(a); 'a'; end}.new.inspect
    # raises ArgumentError, wrong number of arguments (0 for 1)

Interestingly, prior to 1.9.2, ruby did not call to_s if inspect was not defined and there was at least one instance variable in the object.  If the object had no instance variables and inspect was not defined, it called to_s.

Anyway, the bug in Sequel was that Sequel::SQL::Expression objects defined to_s but not inspect, but Sequel::SQL::Expression#to_s required a dataset argument.  So with the 1.9.2 change, Sequel::SQL::Expression#inspect now called to_s, but with no argument, so an ArgumentError was now raised.  This was fixed by <a href="http://github.com/jeremyevans/sequel/commit/752bb1b20b3459b53ef6c3c3a37fa9f0ce90bf67#L0R87">defining a real Sequel::SQL::Expression#inspect method</a>.

I think this is a good change, as it makes the behavior more consistent.

### Array(obj) when obj defines method_missing but not to_ary

While this probably does make things more convenient internally, I can certainly see it causing problems.  What this 1.9.2 change did was to make Array() always call to_ary even if it wasn't explicitly defined.  Which means if you have a method_missing defined for the object, Array(object) will call object.method_missing(:to_ary).  Here's an example:

    # 1.9.1
    Array(Class.new{def method_missing(*a); a; end}.new)
    # => [#<#<Class:0xbd9e40>:0xbd9d60>]

    # 1.9.2
    Array(Class.new{def method_missing(*a); a; end}.new)
    # => [:to_ary]

Thankfully, Sequel only uses method_missing in a few cases, and tries to contain objects that implement method_missing in DSL blocks.  This change mostly affected Sequel's specs, which do use method_missing to make mocking easier, but <a href="http://github.com/jeremyevans/sequel/commit/752bb1b20b3459b53ef6c3c3a37fa9f0ce90bf67#L2R216">it was fairly easy to fix the issues</a>.

### Warnings for class variables in anonymous classes

This isn't really an error, but since ruby now warns about this, it's probably best to change it.  Basically, if you do:

    Class.new{@@a = 1}

Ruby 1.9.2 will give you a warning:

    warning: class variable access from toplevel

This is because anonymous classes created via Class.new work differently in regards to class variable scope than classes created with the class keyword.  You can work around the issue using <a href="http://github.com/jeremyevans/sequel/commit/455eed5774a9417ac77728f182eff541937c57b2">class_variable_get and class_variable_set for anonymous classes</a>.   These are public methods in 1.9, but were private methods in 1.8, so you need to use send to call them from inside the class instances if you want your code to work on both 1.8 and 1.9.

This was another change that only really affected the specs.  While a bit inconvenient, it's not a big deal as not much production code uses anonymous classes with class variables.

I hope this post helps make you aware of some issues you might run into when upgrading to 1.9.2. If you have any issues with running Sequel on 1.9.2, please post in the comments.
