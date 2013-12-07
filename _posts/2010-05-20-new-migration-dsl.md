---
 layout: post
 title: New Migration DSL
---

In this last of a series of blog posts about migrations, I'm going to go over the new DSL for defining migrations:

    Sequel.migration do
      up do
      end

      down do
      end
    end

This is the recommended way of writing migrations going forward.  The previously recommended way is still valid, and will not be deprecated:

    Class.new(Sequel::Migration) do
    # or class SomeMigrationName < Sequel::Migration
      def up
      end

      def down
      end
    end

So what does the new DSL offer?  Well, when originally committed, <a href="http://github.com/jeremyevans/sequel/commit/22a852781ad3e68b11e2ed5347a3374120bd4a7a">it translated into exactly the same class code</a>.  It was actually the first migration related commit in that series, because I wanted its behavior defined and specced before I did the migration refactoring.

Later, after the migration refactoring was complete, I <a href="http://github.com/jeremyevans/sequel/commit/f1b1322019982c0a445a72057df67213f7c2cf8a">modified the Sequel.migration DSL</a> to not create a Sequel::Migration subclass, but instead create an instance of a new class called Sequel::SimpleMigration.  This class is so simple I'm going to inline it:

    class SimpleMigration
      attr_accessor :down, :up

      def apply(db, direction)
        raise(ArgumentError, "Invalid migration direction specified (#{direction.inspect})") unless [:up, :down].include?(direction)
        db.instance_eval(&send(direction))
      end
    end

Previously, where the Sequel.migration DSL would create a new Migration subclass, and the up and down methods in the Sequel.migration block would define the subclass's up and down methods, with that commit, it creates a SimpleMigration instance, and the up and down methods in the Sequel.migration block would just assign their blocks to the up and down attributes.

The end result of this is that the implementation is simpler, and you don't need to use method_missing to proxy methods from the migration subclass to the Database object, you just instance_eval the block provided to up or down on the Database object.
