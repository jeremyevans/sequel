## Sequel: The Database Toolkit for Ruby

Sequel is a simple, flexible, and powerful SQL database access toolkit for
Ruby.

* Sequel provides thread safety, connection pooling and a concise DSL for constructing SQL queries and table schemas.
* Sequel includes a comprehensive ORM layer for mapping records to Ruby objects and handling associated records.
* Sequel supports advanced database features such as prepared statements, bound variables, stored procedures, savepoints, two-phase commit, transaction isolation, master/slave configurations, and database sharding.
* Sequel currently has adapters for ADO, Amalgalite, CUBRID, DataObjects, IBM_DB, JDBC, MySQL, Mysql2, ODBC, Oracle, PostgreSQL, SQLAnywhere, SQLite3, Swift, and TinyTDS.


## Resources
Description | Link
------------ | -------------
Website | http://sequel.jeremyevans.net
RDoc Documentation | http://sequel.jeremyevans.net/rdoc
Source Code | https://github.com/jeremyevans/sequel
Bug tracking (GitHub Issues) | http://github.com/jeremyevans/sequel/issues
Discussion Forum (sequel-talk Google Group) | http://groups.google.com/group/sequel-talk
IRC Channel (#sequel) | irc://irc.freenode.net/sequel


If you have questions about how to use Sequel, please ask on the sequel-talk
Google Group or IRC.  Only use the the bug tracker to report bugs in Sequel,
not to ask for help on using Sequel.

To check out the source code:

    git clone git://github.com/jeremyevans/sequel.git

### Contact

If you have any comments or suggestions please post to the Google group.

## Installation

    gem install sequel

## A Short Example
```ruby
require 'sequel'

DB = Sequel.sqlite # memory database, requires sqlite3

DB.create_table :items do
  primary_key :id
  String :name
  Float :price
end

items = DB[:items] # Create a dataset

# Populate the table
items.insert(:name => 'abc', :price => rand * 100)
items.insert(:name => 'def', :price => rand * 100)
items.insert(:name => 'ghi', :price => rand * 100)

# Print out the number of records
puts "Item count: #{items.count}"

# Print out the average price
puts "The average price is: #{items.avg(:price)}"
```

## The Sequel Console

Sequel includes an IRB console for quick access to databases (usually referred
to as `bin/sequel`). You can use it like this:

    sequel sqlite://test.db # test.db in current directory

You get an IRB session with the database object stored in DB.

In addition to providing an IRB shell (the default behavior), bin/sequel also
has support for migrating databases, dumping schema migrations, and copying
databases.  See the [bin/sequel guide](rdoc-ref:doc/bin_sequel.rdoc) for more
details.

## An Introduction

Sequel is designed to take the hassle away from connecting to databases and
manipulating them. Sequel deals with all the boring stuff like maintaining
connections, formatting SQL correctly and fetching records so you can
concentrate on your application.

Sequel uses the concept of datasets to retrieve data. A Dataset object
encapsulates an SQL query and supports chainability, letting you fetch data
using a convenient Ruby DSL that is both concise and flexible.

For example, the following one-liner returns the average GDP for countries in
the middle east region:
```ruby
DB[:countries].filter(:region => 'Middle East').avg(:GDP)
```
Which is equivalent to:
```ruby
SELECT avg(GDP) FROM countries WHERE region = 'Middle East'
```
Since datasets retrieve records only when needed, they can be stored and later
reused. Records are fetched as hashes (or custom model objects), and are
accessed using an `Enumerable` interface:
```ruby
middle_east = DB[:countries].filter(:region => 'Middle East')
middle_east.order(:name).each{|r| puts r[:name]}
```
Sequel also offers convenience methods for extracting data from Datasets, such
as an extended `map` method:
```ruby
middle_east.map(:name) #=> ['Egypt', 'Turkey', 'Israel', ...]
```
Or getting results as a hash via `to_hash`, with one column as key and another
as value:
```ruby
middle_east.to_hash(:name, :area) #=> {'Israel' => 20000, 'Turkey' => 120000, ...}
```
## Getting Started

### Connecting to a database

To connect to a database you simply provide `Sequel.connect` with a URL:
```ruby
require 'sequel'
DB = Sequel.connect('sqlite://blog.db') # requires sqlite3
```
The connection URL can also include such stuff as the user name, password, and
port:
```ruby
DB = Sequel.connect('postgres://user:password@host:port/database_name') # requires pg
```
You can also specify optional parameters, such as the connection pool size, or
loggers for logging SQL queries:
```ruby
DB = Sequel.connect("postgres://user:password@host:port/database_name",
  :max_connections => 10, :logger => Logger.new('log/db.log'))
```
You can specify a block to connect, which will disconnect from the database
after it completes:
```ruby
Sequel.connect('postgres://user:password@host:port/database_name'){|db| db[:posts].delete}
```
### The DB convention

Throughout Sequel's documentation, you will see the `DB` constant used to
refer to the Sequel::Database instance you create. This reflects the
recommendation that for an app with a single Sequel::Database instance, the
Sequel convention is to store the instance in the `DB` constant.  This is just
a convention, it's not required, but it is recommended.

Note that some frameworks that use Sequel may create the Sequel::Database
instance for you, and you might not know how to access it.  In most cases, you
can access the Sequel::Database instance through `Sequel::Model.db`.

### Arbitrary SQL queries

You can execute arbitrary SQL code using `Database#run`:
```ruby
DB.run("create table t (a text, b text)")
DB.run("insert into t values ('a', 'b')")
```
You can also create datasets based on raw SQL:
```ruby
dataset = DB['select id from items']
dataset.count # will return the number of records in the result set
dataset.map(:id) # will return an array containing all values of the id column in the result set
```
You can also fetch records with raw SQL through the dataset:
```ruby
DB['select * from items'].each do |row|
  p row
end
```
You can use placeholders in your SQL string as well:
```ruby
name = 'Jim'
DB['select * from items where name = ?', name].each do |row|
  p row
end
```
### Getting Dataset Instances

Datasets are the primary way records are retrieved and manipulated.  They are
generally created via the `Database#from` or `Database#[]` methods:
```ruby
posts = DB.from(:posts)
posts = DB[:posts] # same
```
Datasets will only fetch records when you tell them to. They can be
manipulated to filter records, change ordering, join tables, etc..

### Retrieving Records

You can retrieve all records by using the `all` method:
```ruby
posts.all
# SELECT * FROM posts
```
The all method returns an array of hashes, where each hash corresponds to a
record.

You can also iterate through records one at a time using `each`:
```ruby
posts.each{|row| p row}
```
Or perform more advanced stuff:
```ruby
names_and_dates = posts.map([:name, :date])
old_posts, recent_posts = posts.partition{|r| r[:date] < Date.today - 7}
```
You can also retrieve the first record in a dataset:
```ruby
posts.first
# SELECT * FROM posts LIMIT 1
```
Or retrieve a single record with a specific value:
```ruby
posts[:id => 1]
# SELECT * FROM posts WHERE id = 1 LIMIT 1
```
If the dataset is ordered, you can also ask for the last record:
```ruby
posts.order(:stamp).last
# SELECT * FROM posts ORDER BY stamp DESC LIMIT 1
```
### Filtering Records

An easy way to filter records is to provide a hash of values to match to
`where`:
```ruby
my_posts = posts.where(:category => 'ruby', :author => 'david')
# WHERE category = 'ruby' AND author = 'david'
```
You can also specify ranges:
```ruby
my_posts = posts.where(:stamp => (Date.today - 14)..(Date.today - 7))
# WHERE stamp >= '2010-06-30' AND stamp <= '2010-07-07'
```
Or arrays of values:
```ruby
my_posts = posts.where(:category => ['ruby', 'postgres', 'linux'])
# WHERE category IN ('ruby', 'postgres', 'linux')
```
Sequel also accepts expressions:
```ruby
my_posts = posts.where{stamp > Date.today << 1}
# WHERE stamp > '2010-06-14'
my_posts = posts.where{stamp =~ Date.today}
# WHERE stamp = '2010-07-14'
```
Some adapters will also let you specify Regexps:
```ruby
my_posts = posts.where(:category => /ruby/i)
# WHERE category ~* 'ruby'
```
You can also use an inverse filter via `exclude`:
```ruby
my_posts = posts.exclude(:category => ['ruby', 'postgres', 'linux'])
# WHERE category NOT IN ('ruby', 'postgres', 'linux')
```
You can also specify a custom WHERE clause using a string:
```ruby
posts.where('stamp IS NOT NULL')
# WHERE stamp IS NOT NULL
```
You can use parameters in your string, as well:
```ruby
author_name = 'JKR'
posts.where('(stamp < ?) AND (author != ?)', Date.today - 3, author_name)
# WHERE (stamp < '2010-07-11') AND (author != 'JKR')
```
Datasets can also be used as subqueries:
```ruby
DB[:items].where('price > ?', DB[:items].select{avg(price) + 100})
# WHERE price > (SELECT avg(price) + 100 FROM items)
```
After filtering, you can retrieve the matching records by using any of the
retrieval methods:
```ruby
my_posts.each{|row| p row}
```
See the [Dataset Filtering](rdoc-ref:doc/dataset_filtering.rdoc) file for more
details.

### Security

Designing apps with security in mind is a best practice. Please read the
[Security Guide](rdoc-ref:doc/security.rdoc) for details on security issues
that you should be aware of when using Sequel.

### Summarizing Records

Counting records is easy using `count`:
```ruby
posts.where(Sequel.like(:category, '%ruby%')).count
# SELECT COUNT(*) FROM posts WHERE category LIKE '%ruby%'
```
And you can also query maximum/minimum values via `max` and `min`:
```ruby
max = DB[:history].max(:value)
# SELECT max(value) FROM history

min = DB[:history].min(:value)
# SELECT min(value) FROM history
```
Or calculate a sum or average via `sum` and `avg`:
```ruby
sum = DB[:items].sum(:price)
# SELECT sum(price) FROM items
avg = DB[:items].avg(:price)
# SELECT avg(price) FROM items
```
### Ordering Records

Ordering datasets is simple using `order`:
```ruby
posts.order(:stamp)
# ORDER BY stamp
posts.order(:stamp, :name)
# ORDER BY stamp, name
```
Chaining `order` doesn't work the same as `where`:
```ruby
posts.order(:stamp).order(:name)
# ORDER BY name
```
The `order_append` method chains this way, though:
```ruby
posts.order(:stamp).order_append(:name)
# ORDER BY stamp, name
```
The `order_prepend` method can be used as well:
```ruby
posts.order(:stamp).order_prepend(:name)
# ORDER BY name, stamp
```
You can also specify descending order:
```ruby
posts.reverse_order(:stamp)
# ORDER BY stamp DESC
posts.order(Sequel.desc(:stamp))
# ORDER BY stamp DESC
```
### Core Extensions

Note the use of `Sequel.desc(:stamp)` in the above example.  Much of Sequel's
DSL uses this style, calling methods on the Sequel module that return SQL
expression objects.  Sequel also ships with a [core_extensions
extension](rdoc-ref:doc/core_extensions.rdoc)) that integrates Sequel's DSL
better into the ruby language, allowing you to write:
```ruby
:stamp.desc
```
instead of:
```ruby
Sequel.desc(:stamp)
```
### Selecting Columns

Selecting specific columns to be returned is also simple using `select`:
```ruby
posts.select(:stamp)
# SELECT stamp FROM posts
posts.select(:stamp, :name)
# SELECT stamp, name FROM posts
```
Chaining `select` works like `order`, not `where`:
```ruby
posts.select(:stamp).select(:name)
# SELECT name FROM posts
```
As you might expect, there is an `order_append` equivalent for `select` called
`select_append`:
```ruby
posts.select(:stamp).select_append(:name)
# SELECT stamp, name FROM posts
```
### Deleting Records

Deleting records from the table is done with `delete`:
```ruby
posts.where('stamp < ?', Date.today - 3).delete
# DELETE FROM posts WHERE stamp < '2010-07-11'
```
Be very careful when deleting, as `delete` affects all rows in the dataset.
Call `where` first and `delete` second:
```ruby
# DO THIS:
posts.where('stamp < ?', Date.today - 7).delete
# NOT THIS:
posts.delete.where('stamp < ?', Date.today - 7)
```
### Inserting Records

Inserting records into the table is done with `insert`:
```ruby
posts.insert(:category => 'ruby', :author => 'david')
# INSERT INTO posts (category, author) VALUES ('ruby', 'david')
```
### Updating Records

Updating records in the table is done with `update`:
```ruby
posts.where('stamp < ?', Date.today - 7).update(:state => 'archived')
# UPDATE posts SET state = 'archived' WHERE stamp < '2010-07-07'
```
You can reference table columns when choosing what values to set:
```ruby
posts.where{|o| o.stamp < Date.today - 7}.update(:backup_number => Sequel.+(:backup_number, 1))
# UPDATE posts SET backup_number = backup_number + 1 WHERE stamp < '2010-07-07'
```
As with `delete`, `update` affects all rows in the dataset, so `where` first,
`update` second:
```ruby
# DO THIS:
posts.where('stamp < ?', Date.today - 7).update(:state => 'archived')
# NOT THIS:
posts.update(:state => 'archived').where('stamp < ?', Date.today - 7)
```
### Transactions

You can wrap some code in a database transaction using the
`Database#transaction` method:
```ruby
DB.transaction do
  posts.insert(:category => 'ruby', :author => 'david')
  posts.where('stamp < ?', Date.today - 7).update(:state => 'archived')
end
```
If the block does not raise an exception, the transaction will be committed.
If the block does raise an exception, the transaction will be rolled back, and
the exception will be reraised.  If you want to rollback the transaction and
not raise an exception outside the block, you can raise the `Sequel::Rollback`
exception inside the block:
```ruby
DB.transaction do
  posts.insert(:category => 'ruby', :author => 'david')
  if posts.filter('stamp < ?', Date.today - 7).update(:state => 'archived') == 0
    raise Sequel::Rollback
  end
end
```
### Joining Tables

Sequel makes it easy to join tables:
```ruby
order_items = DB[:items].join(:order_items, :item_id => :id).
  where(:order_id => 1234)
# SELECT * FROM items INNER JOIN order_items
# ON order_items.item_id = items.id
# WHERE order_id = 1234
```
The important thing to note here is that item_id is automatically qualified
with the table being joined, and id is automatically qualified with the last
table joined.

You can then do anything you like with the dataset:
```ruby
order_total = order_items.sum(:price)
# SELECT sum(price) FROM items INNER JOIN order_items
# ON order_items.item_id = items.id
# WHERE order_items.order_id = 1234
```
Note that the default selection in Sequel is `*`, which includes all columns
in all joined tables.  Because Sequel returns results as a hash keyed by
column name symbols, if any tables have columns with the same name, this will
clobber the columns in the returned hash.  So when joining you are usually
going to want to change the selection using `select`, `select_all`, and/or
`select_append`.

## Column references in Sequel

Sequel expects column names to be specified using symbols. In addition,
returned hashes always use symbols as their keys. This allows you to freely
mix literal values and column references in many cases. For example, the two
following lines produce equivalent SQL:
```ruby
items.where(:x => 1)
# SELECT * FROM items WHERE (x = 1)
items.where(1 => :x)
# SELECT * FROM items WHERE (1 = x)"
```
Ruby strings are generally treated as SQL strings:
```ruby
items.where(:x => 'x')
# SELECT * FROM items WHERE (x = 'x')
```
### Qualifying identifiers (column/table names)

An identifier in SQL is a name that represents a column, table, or schema.
Identifiers can be qualified by using the double underscore special notation
`:table__column`:
```ruby
items.literal(:items__price)
# items.price
```
Another way to qualify columns is to use the `Sequel[][]` method:
```ruby
items.literal(Sequel[:items][:price])
# items.price
```
While it is more common to qualify column identifiers with table identifiers,
you can also qualify table identifiers with schema identifiers to select from
a qualified table:
```ruby
posts = DB[:some_schema__posts]
# SELECT * FROM some_schema.posts
```
### Identifier aliases

You can also alias identifiers by using the triple underscore special notation
`:column___alias` or `:table__column___alias`:
```ruby
items.literal(:price___p)
# price AS p
items.literal(:items__price___p)
# items.price AS p
```
Another way to alias columns is to use the `Sequel.as` method:
```ruby
items.literal(Sequel.as(:price, :p))
# price AS p
```
You can use the `Sequel.as` method to alias arbitrary expressions, not just
identifiers:
```ruby
items.literal(Sequel.as(DB[:posts].select{max(id)}, :p))
# (SELECT max(id) FROM posts) AS p
```
## Sequel Models

A model class wraps a dataset, and an instance of that class wraps a single
record in the dataset.

Model classes are defined as regular Ruby classes inheriting from
`Sequel::Model`:
```ruby
DB = Sequel.connect('sqlite://blog.db')
class Post < Sequel::Model
end
```
When a model class is created, it parses the schema in the table from the
database, and automatically sets up accessor methods for all of the columns in
the table (Sequel::Model implements the active record pattern).

Sequel model classes assume that the table name is an underscored plural of
the class name:
```ruby
Post.table_name #=> :posts
```
You can explicitly set the table name or even the dataset used:
```ruby
class Post < Sequel::Model(:my_posts)
end
# or:
Post.set_dataset :my_posts
```
If you call `set_dataset` with a symbol, it assumes you are referring to the
table with the same name.  You can also call it with a dataset, which will set
the defaults for all retrievals for that model:
```ruby
Post.set_dataset DB[:my_posts].where(:category => 'ruby')
Post.set_dataset DB[:my_posts].select(:id, :name).order(:date)
```
### Model instances

Model instances are identified by a primary key.  In most cases, Sequel can
query the database to determine the primary key, but if not, it defaults to
using `:id`. The `Model.[]` method can be used to fetch records by their
primary key:
```ruby
post = Post[123]
```
The `pk` method is used to retrieve the record's primary key value:
```ruby
post.pk #=> 123
```
Sequel models allow you to use any column as a primary key, and even composite
keys made from multiple columns:
```ruby
class Post < Sequel::Model
  set_primary_key [:category, :title]
end

post = Post['ruby', 'hello world']
post.pk #=> ['ruby', 'hello world']
```
You can also define a model class that does not have a primary key via
`no_primary_key`, but then you lose the ability to easily update and delete
records:
```ruby
Post.no_primary_key
```

A single model instance can also be fetched by specifying a condition:
```ruby
post = Post[:title => 'hello world']
post = Post.first{num_comments < 10}
```
### Acts like a dataset

A model class forwards many methods to the underlying dataset. This means that
you can use most of the `Dataset` API to create customized queries that return
model instances, e.g.:
```ruby
Post.where(:category => 'ruby').each{|post| p post}
```
You can also manipulate the records in the dataset:
```ruby
Post.where{num_comments < 7}.delete
Post.where(Sequel.like(:title, /ruby/)).update(:category => 'ruby')
```
### Accessing record values

A model instance stores its values as a hash with column symbol keys, which
you can access directly via the `values` method:
```ruby
post.values #=> {:id => 123, :category => 'ruby', :title => 'hello world'}
```
You can read the record values as object attributes, assuming the attribute
names are valid columns in the model's dataset:
```ruby
post.id #=> 123
post.title #=> 'hello world'
```
If the record's attributes names are not valid columns in the model's dataset
(maybe because you used `select_append` to add a computed value column), you
can use `Model#[]` to access the values:
```ruby
post[:id] #=> 123
post[:title] #=> 'hello world'
```
You can also modify record values using attribute setters or the `[]=` method.
```ruby
post.title = 'hey there'
post[:title] = 'hey there'
```
That will just change the value for the object, it will not update the row in
the database.  To update the database row, call the `save` method:
```ruby
post.save
```
### Mass assignment

You can also set the values for multiple columns in a single method call,
using one of the mass-assignment methods.  See the [mass assignment
guide](rdoc-ref:doc/mass_assignment.rdoc) for details.  For example `set`
updates the model's column values without saving:
```ruby
post.set(:title=>'hey there', :updated_by=>'foo')
```
and `update` updates the model's column values and then saves the changes to
the database:
```ruby
post.update(:title => 'hey there', :updated_by=>'foo')
```
### Creating new records

New records can be created by calling `Model.create`:
```ruby
post = Post.create(:title => 'hello world')
```
Another way is to construct a new instance and save it later:
```ruby
post = Post.new
post.title = 'hello world'
post.save
```
You can also supply a block to `Model.new` and `Model.create`:
```ruby
post = Post.new do |p|
  p.title = 'hello world'
end

post = Post.create{|p| p.title = 'hello world'}
```
### Hooks

You can execute custom code when creating, updating, or deleting records by
defining hook methods. The `before_create` and `after_create` hook methods
wrap record creation. The `before_update` and `after_update` hook methods wrap
record updating. The `before_save` and `after_save` hook methods wrap record
creation and updating. The `before_destroy` and `after_destroy` hook methods
wrap destruction. The `before_validation` and `after_validation` hook methods
wrap validation. Example:
```ruby
class Post < Sequel::Model
  def after_create
    super
    author.increase_post_count
  end

  def after_destroy
    super
    author.decrease_post_count
  end
end
```
Note the use of `super` if you define your own hook methods.  Almost all
`Sequel::Model` class and instance methods (not just hook methods) can be
overridden safely, but you have to make sure to call `super` when doing so,
otherwise you risk breaking things.

For the example above, you should probably use a database trigger if you can.
Hooks can be used for data integrity, but they will only enforce that
integrity when you are modifying the database through model instances, and
even then they are often subject to race conditions.  It's best to use
database triggers and constraints to enforce data integrity.

### Deleting records

You can delete individual records by calling `delete` or `destroy`. The only
difference between the two methods is that `destroy` invokes `before_destroy`
and `after_destroy` hook methods, while `delete` does not:
```ruby
post.delete # => bypasses hooks
post.destroy # => runs hooks
```
Records can also be deleted en-masse by calling `delete` and `destroy` on the
model's dataset. As stated above, you can specify filters for the deleted
records:
```ruby
Post.where(:category => 32).delete # => bypasses hooks
Post.where(:category => 32).destroy # => runs hooks
```
Please note that if `destroy` is called, each record is deleted  separately,
but `delete` deletes all matching records with a single  SQL query.

### Associations

Associations are used in order to specify relationships between model classes
that reflect relationships between tables in the database, which are usually
specified using foreign keys.  You specify model associations via class
methods:
```ruby
class Post < Sequel::Model
  many_to_one :author
  one_to_many :comments
  one_to_one :first_comment, :class=>:Comment, :order=>:id
  many_to_many :tags
  one_through_one :first_tag, :class=>:Tag, :order=>:name, :right_key=>:tag_id
end
```
`many_to_one` and `one_to_one` create a getter and setter for each model
object:
```ruby
post = Post.create(:name => 'hi!')
post.author = Author[:name => 'Sharon']
post.author
```
`one_to_many` and `many_to_many` create a getter method, a method for adding
an object to the association, a method for removing an object from the
association, and a method for removing all associated objects from the
association:
```ruby
post = Post.create(:name => 'hi!')
post.comments

comment = Comment.create(:text=>'hi')
post.add_comment(comment)
post.remove_comment(comment)
post.remove_all_comments

tag = Tag.create(:tag=>'interesting')
post.add_tag(tag)
post.remove_tag(tag)
post.remove_all_tags
```
Note that the remove_* and remove_all_* methods do not delete the object from
the database, they merely disassociate the associated object from the
receiver.

All associations add a dataset method that can be used to further filter or
reorder the returned objects, or modify all of them:

    # Delete all of this post's comments from the database
    post.comments_dataset.destroy

    # Return all tags related to this post with no subscribers, ordered by the tag's name
    post.tags_dataset.where(:subscribers=>0).order(:name).all

### Eager Loading

Associations can be eagerly loaded via `eager` and the `:eager` association
option. Eager loading is used when loading a group of objects. It loads all
associated objects for all of the current objects in one query, instead of
using a separate query to get the associated objects for each current object.
Eager loading requires that you retrieve all model objects at once via `all`
(instead of individually by `each`). Eager loading can be cascaded, loading
association's associated objects.
```ruby
class Person < Sequel::Model
  one_to_many :posts, :eager=>[:tags]
end

class Post < Sequel::Model
  many_to_one :person
  one_to_many :replies
  many_to_many :tags
end

class Tag < Sequel::Model
  many_to_many :posts
  many_to_many :replies
end

class Reply < Sequel::Model
  many_to_one :person
  many_to_one :post
  many_to_many :tags
end

# Eager loading via .eager
Post.eager(:person).all

# eager is a dataset method, so it works with filters/orders/limits/etc.
Post.where{topic > 'M'}.order(:date).limit(5).eager(:person).all

person = Person.first
# Eager loading via :eager (will eagerly load the tags for this person's posts)
person.posts

# These are equivalent
Post.eager(:person, :tags).all
Post.eager(:person).eager(:tags).all

# Cascading via .eager
Tag.eager(:posts=>:replies).all

# Will also grab all associated posts' tags (because of :eager)
Reply.eager(:person=>:posts).all

# No depth limit (other than memory/stack), and will also grab posts' tags
# Loads all people, their posts, their posts' tags, replies to those posts,
# the person for each reply, the tag for each reply, and all posts and
# replies that have that tag.  Uses a total of 8 queries.
Person.eager(:posts=>{:replies=>[:person, {:tags=>[:posts, :replies]}]}).all
```
In addition to using `eager`, you can also use `eager_graph`, which will use a
single query to get the object and all associated objects.  This may be
necessary if you want to filter or order the result set based on columns in
associated tables.  It works with cascading as well, the API is very similar.
Note that using `eager_graph` to eagerly load multiple `*_to_many`
associations will cause the result set to be a cartesian product, so you
should be very careful with your filters when using it in that case.

You can dynamically customize the eagerly loaded dataset by using using a
proc.  This proc is passed the dataset used for eager loading, and should
return a modified copy of that dataset:
```ruby
# Eagerly load only replies containing 'foo'
Post.eager(:replies=>proc{|ds| ds.where(Sequel.like(text, '%foo%'))}).all
```
This also works when using `eager_graph`, in which case the proc is called
with dataset to graph into the current dataset:
```ruby
Post.eager_graph(:replies=>proc{|ds| ds.where(Sequel.like(text, '%foo%'))}).all
```
You can dynamically customize eager loads for both `eager` and `eager_graph`
while also cascading, by making the value a single entry hash with the proc as
a key, and the cascaded associations as the value:
```ruby
# Eagerly load only replies containing 'foo', and the person and tags for those replies
Post.eager(:replies=>{proc{|ds| ds.where(Sequel.like(text, '%foo%'))}=>[:person, :tags]}).all
```
### Joining with Associations

You can use the association_join method to add a join to the model's dataset
based on the assocation:
```ruby
Post.association_join(:author)
# SELECT * FROM posts
# INNER JOIN authors AS author ON (author.id = posts.author_id)
```
This comes with variants for different join types:
```ruby
Post.association_left_join(:replies)
# SELECT * FROM posts
# LEFT JOIN replies ON (replies.post_id = posts.id)
```
Similar to the eager loading methods, you can use multiple associations and
nested associations:
```ruby
Post.association_join(:author, :replies=>:person).all
# SELECT * FROM posts
# INNER JOIN authors AS author ON (author.id = posts.author_id)
# INNER JOIN replies ON (replies.post_id = posts.id)
# INNER JOIN people AS person ON (person.id = replies.person_id)
```
### Extending the underlying dataset

The recommended way to implement table-wide logic by defining methods on the
dataset using `dataset_module`:
```ruby
class Post < Sequel::Model
  dataset_module do
    def posts_with_few_comments
      where{num_comments < 30}
    end

    def clean_posts_with_few_comments
      posts_with_few_comments.delete
    end
  end
end
```
This allows you to have access to your model API from filtered datasets as
well:
```ruby
Post.where(:category => 'ruby').clean_posts_with_few_comments
```
Sequel models also provide a `subset` class method that creates a dataset
method with a simple filter:
```ruby
class Post < Sequel::Model
  subset(:posts_with_few_comments){num_comments < 30}
  subset :invisible, Sequel.~(:visible)
end
```
### Model Validations

You can define a `validate` method for your model, which `save` will check
before attempting to save the model in the database. If an attribute of the
model isn't valid, you should add an error message for that attribute to the
model object's `errors`. If an object has any errors added by the validate
method, `save` will raise an error or return false depending on how it is
configured (the `raise_on_save_failure` flag).
```ruby
class Post < Sequel::Model
  def validate
    super
    errors.add(:name, "can't be empty") if name.empty?
    errors.add(:written_on, "should be in the past") if written_on >= Time.now
  end
end
```
