---
 layout: post
 title: Sequel::BasicObject and ruby 1.8
---

<a href="http://github.com/jeremyevans/sequel/commit/0674811d4db5e66f7a0e2bc92f3ea697fbcac775">Sequel::BasicObject was added in July of 2009</a>, mostly as an implementaion detail of the association_proxies plugin.  However, the main idea dates back to the <a href="http://github.com/jeremyevans/sequel/commit/9b29c06dd4b81306be14eec16e15b7b321926043">addition of the VirtualRow support in July of 2008</a>, from which Sequel::BasicObject was extracted.

Sequel::BasicObject provides Sequel with a ruby 1.9 style BasicObject class that proxy classes can be based on.  On ruby 1.9, its only advantage over BasicObject is that it handles constant lookup misses by checking for constants defined in Object, allowing the following type of code to work:

    DB[:logs].filter{added_on > Time.now}

Using the standard BasicObject class, such a query fails because BasicObject::Time is not defined, and BasicObject cannot directly access constants defined in Object, which is where all other top level constants are defined by default.

On ruby 1.8, Sequel::BasicObject exists to serve a similar role as the standard BasicObject does in 1.9.  On 1.8, constant lookup isn't an issue, so the only thing to worry about is how to remove methods that are defined in Object.  Previously, Sequel used a fairly simple solution for removing methods:

    module Sequel  
      class BasicObject
        m = %w"__id__ __send__ instance_eval == equal?"
        (instance_methods - m).each{|m| undef_method(m)}
      end
    end

This works OK for simple cases, but it has a couple of problems.  The most obvious problem is that the methods that are added after Sequel is required are not removed:

    require 'sequel'
    class Object
      def b
        42
      end
    end
    DB[:a].filter{a > b}
    # a > 42 instead of a > b

There's two ways to deal with this.  The heavy handed way is using method_added hooks to try to remove the methods as soon as they are added.  Unfortunately, this is not really a workable solution, since you'd need to add the method_added hooks to Object and all modules included in Object (overriding Object.include to keep track of future modules included in Object).  The simpler way is to just provide a method that you can call anytime that will remove any unnecessary instance methods from Sequel::BasicObject.  I've chosen the simpler route, so now you can do:

    require 'sequel'
    class Object
      def b
        42
      end
    end
    Sequel::BasicObject.remove_methods!
    DB[:a].filter{a > b}
    # a > b

The less obvious issue with the code is that instance_methods doesn't actually include all methods.  In ruby, instance_methods only includes public and protected instance methods, it does not include private instance methods.  Which means that the following type of code raised an error:

    DB[:a].filter{a > p}

This is because Kernel#p is a private method, which was not removed by the above code.  In this case, an error is raised because although p accepts 0 arguments, it returns nil, and Sequel knows that a > NULL is not valid SQL, so it raises an error. Now, Sequel uses something similar to:

    module Sequel  
      class BasicObject
        KEEP_METHODS = %w"__id__ __send__ instance_eval == equal? initialize"

        def self.remove_methods!
          m = (private_instance_methods + instance_methods) - KEEP_METHODS
          m.each{|m| undef_method(m)}
        end
        remove_methods!
      end
    end

This removes all instance methods, regardless of visibility, other than the ones specifically excluded.

Adding a BasicObject class to the language is one of the best decisions made in ruby 1.9, but hopefully this shows how you can still get similar behavior from 1.8.
