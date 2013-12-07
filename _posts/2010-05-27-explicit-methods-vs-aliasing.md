---
 layout: post
 title: Explicit Methods Vs. Aliasing
---

It's a fairly common practice in ruby to add aliases for methods, so that the same method can be referred to by two different names.  It's so common that ruby has a keyword for this, so to if you have a method such as:

    def one
      []
    end

And you want uno to call the same method, you just do the following:

    alias uno one

Note that you use a literal name when aliasing, so the alias keyword can only alias methods whose names can be literals.  Also note that you don't separate the the two method names with a comma, as it isn't a regular method call.

If you define a method with a name that is not a valid literal, such as:

    define_method(:"eleventy-one") do
      111
    end

And you want to alias it to "cent onze", you need to use alias_method:

    alias_method :"cent onze", :"eleventy-one"

alias_method is not a keyword, it's a private instance method of the Module class, so it uses regular method call syntax, where the arguments are separated with commas.  Unlike alias, alias_method takes symbols instead of literals, so it can be used to alias methods whose names aren't valid literals.

Note that aliasing methods is not the same as creating one method that calls the other method.  When you do:

    alias uno one

it does not do:

    def uno
      one
    end

instead, it does:

    def uno
      []
    end
  
Basically, an alias adds a new method that uses the same method body as the method being aliased.  Now most of the time, that is what you want.  However, when creating subclass hierarchies, sometimes it is what you want, and sometimes it isn't.  Think about this code:

    class A
      def position
        1
      end
      alias positie position
    end

    class B < A
      def position
        2
      end
    end

Basically, you have a class hierarchy, with two separate names for a method.  In lower levels of the class hierarchy, you override the method to give the class specific behavior.  Unfortunately, this doesn't work well:

    a = A.new
    b = B.new

    a.position # => 1
    b.position # => 2
    a.positie # => 1
    b.positie # => 1 !!!

Note the incorrect result for b.positie.  This is because B#positie is not defined in B, so it calls A#positie, using ruby's usual method lookup.  Unfortunately, because you used alias, what you really did is this:

    class A
      def positie
        1
      end
    end

Which is why b.positie gives you 1.  The correct solution in this case is not to use method aliasing, but to create actual methods that call the methods they are supposed to alias:

    class A
      def position
        1
      end

      def positie
        position
      end
    end

    class B < A
      def position
        2
      end
    end

This gives you correct results:

    a.position # => 1
    b.position # => 2
    a.positie # => 1
    b.positie # => 2

Now, that does not mean that using explicit methods is better than aliasing in all cases, you need to use your judgment and make a decision on a case by case basis.  If you have methods a and b, and they are always supposed to mean the same thing, an explicit method is probably better.  However, if you have methods c and d, which happen to do the same thing, but are not required to do so, then an alias is probably better.

So what does this have to do with Sequel?  Well, I recently made some modifications to Sequel <a href="http://github.com/jeremyevans/sequel/commit/3670ddcdd787c0a28027f03f73ed0a733f6ad403">to use explicit methods over aliasing in cases where it makes sense</a>.
