---
 layout: post
 title: More ActiveRecord Pilfering
---

After pilfering <a href="https://github.com/tenderlove">Aaron Patterson's</a> to_dot idea <a href="/2010/11/17/the-sincerest-form.html">last week</a>, I decided to pilfer another good idea he recently added to ActiveRecord, <a href="https://github.com/rails/rails/compare/deff5289474d966bb12a...a4d9b1d3">reversible migrations</a>.

The idea behind reversible migrations is that in many cases, the library can know exactly how to reverse the migration you are using.  For example, if you are using create_table, it can be reversed by using drop_table.  And if you are using create_index followed add_column, it can reverse it by calling drop_column and then drop_index.  Basically, all reversible migrations can be reversed by applying the reverse of each action in reverse order.

<a href="https://github.com/jeremyevans/sequel/commit/94450d775dfdc1b6cc0393944198bfa2ea0ecd71">Sequel implements reversible migrations using a change block inside of a Sequel.migration block</a>:

    Sequel.migration do
      change do
        create_table(:artists) do
          primary_key :id
          String :name, :null=>false
        end
      end
    end

This will automatically create the equivalent of:

    Sequel.migration do
      up do
        create_table(:artists) do
          primary_key :id
          String :name, :null=>false
        end
      end

      down do
        drop_table(:artists)
      end
    end

There is no support for reversible migrations using the historical (but still supported) usage of creating a subclass of Sequel::Migration.

The following Database methods are handled and can be reversed successfully:

* +create_table+
* +add_column+
* +add_index+
* +rename_column+
* +rename_table+
* +alter_table+ (supporting the following methods in the +alter_table+ block):
  * +add_column+
  * +add_constraint+
  * +add_foreign_key+ (with a symbol, not an array)
  * +add_primary_key+ (with a symbol, not an array)
  * +add_index+
  * +add_full_text_index+
  * +add_spatial_index+
  * +rename_column+

Usage of any other method in the change block will result in a down block created that raises a Sequel::Error (basically making the migration irreversible).
