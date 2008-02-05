module Sequel
  # == Sequel Models
  # 
  # Models in Sequel are based on the Active Record pattern described by Martin Fowler (http://www.martinfowler.com/eaaCatalog/activeRecord.html). A model class corresponds to a table or a dataset, and an instance of that class wraps a single record in the model's underlying dataset.
  # 
  # Model classes are defined as regular Ruby classes:
  # 
  #   DB = Sequel('sqlite:/blog.db')
  #   class Post < Sequel::Model
  #     set_dataset DB[:posts]
  #   end
  # 
  # You can also use the shorthand form:
  # 
  #   DB = Sequel('sqlite:/blog.db')
  #   class Post < Sequel::Model
  #   end
  # 
  # === Model instances
  # 
  # Model instance are identified by a primary key. By default, Sequel assumes the primary key column to be :id. The Model#[] method can be used to fetch records by their primary key:
  # 
  #   post = Post[123]
  # 
  # The Model#pk method is used to retrieve the record's primary key value:
  # 
  #   post.pk #=> 123
  # 
  # Sequel models allow you to use any column as a primary key, and even composite keys made from multiple columns:
  # 
  #   class Post < Sequel::Model
  #     set_primary_key [:category, :title]
  #   end
  # 
  #   post = Post['ruby', 'hello world']
  #   post.pk #=> ['ruby', 'hello world']
  # 
  # You can also define a model class that does not have a primary key, but then you lose the ability to update records.
  # 
  # A model instance can also be fetched by specifying a condition:
  # 
  #   post = Post[:title => 'hello world']
  #   post = Post.find {:stamp < 10.days.ago}
  # 
  # === Iterating over records
  # 
  # A model class lets you iterate over specific records by acting as a proxy to the underlying dataset. This means that you can use the entire Dataset API to create customized queries that return model instances, e.g.:
  # 
  #   Post.filter(:category => 'ruby').each {|post| p post}
  # 
  # You can also manipulate the records in the dataset:
  # 
  #   Post.filter {:stamp < 7.days.ago}.delete
  #   Post.filter {:title =~ /ruby/}.update(:category => 'ruby')
  # 
  # === Accessing record values
  # 
  # A model instances stores its values as a hash:
  # 
  #   post.values #=> {:id => 123, :category => 'ruby', :title => 'hello world'}
  # 
  # You can read the record values as object attributes:
  # 
  #   post.id #=> 123
  #   post.title #=> 'hello world'
  # 
  # You can also change record values:
  # 
  #   post.title = 'hey there'
  #   post.save
  # 
  # Another way to change values by using the #set method:
  # 
  #   post.set(:title => 'hey there')
  # 
  # === Creating new records
  # 
  # New records can be created by calling Model.create:
  # 
  #   post = Post.create(:title => 'hello world')
  # 
  # Another way is to construct a new instance and save it:
  # 
  #   post = Post.new
  #   post.title = 'hello world'
  #   post.save
  # 
  # You can also supply a block to Model.new and Model.create:
  # 
  #   post = Post.create {|p| p.title = 'hello world'}
  # 
  #   post = Post.new do |p|
  #     p.title = 'hello world'
  #     p.save
  #   end
  # 
  # === Hooks
  # 
  # You can execute custom code when creating, updating, or deleting records by using hooks. The before_create and after_create hooks wrap record creation. The before_update and after_update wrap record updating. The before_save and after_save wrap record creation and updating. The before_destroy and after_destroy wrap destruction.
  # 
  # Hooks are defined by supplying a block:
  # 
  #   class Post < Sequel::Model
  #     after_create do
  #       set(:created_at => Time.now)
  #     end
  # 
  #     after_destroy do
  #       author.update_post_count
  #     end
  #   end
  # 
  # === Deleting records
  # 
  # You can delete individual records by calling #delete or #destroy. The only difference between the two methods is that #destroy invokes before_destroy and after_destroy hooks, while #delete does not:
  # 
  #   post.delete #=> bypasses hooks
  #   post.destroy #=> runs hooks
  # 
  # Records can also be deleted en-masse by invoking Model.delete and Model.destroy. As stated above, you can specify filters for the deleted records:
  # 
  #   Post.filter(:category => 32).delete #=> bypasses hooks
  #   Post.filter(:category => 32).destroy #=> runs hooks
  # 
  # Please note that if Model.destroy is called, each record is deleted separately, but Model.delete deletes all relevant records with a single SQL statement.
  # 
  # === Associations
  # 
  # The most straightforward way to define an association in a Sequel model is as a regular instance method:
  # 
  #   class Post < Sequel::Model
  #     def author; Author[author_id]; end
  #   end
  # 
  #   class Author < Sequel::Model
  #     def posts; Post.filter(:author_id => pk); end
  #   end
  # 
  # Sequel also provides two macros to assist with common types of associations. The one_to_one macro is roughly equivalent to ActiveRecord?'s belongs_to macro. It defines both getter and setter methods for the association:
  # 
  #   class Post < Sequel::Model
  #     one_to_one :author, :from => Author
  #   end
  #
  #   post = Post.create(:name => 'hi!')
  #   post.author = Author[:name => 'Sharon']
  # 
  # The one_to_many macro is roughly equivalent to ActiveRecord's has_many macro:
  # 
  #   class Author < Sequel::Model
  #     one_to_many :posts, :from => Post, :key => :author_id
  #   end
  # 
  # You will have noticed that in some cases the association macros are actually more verbose than hand-coding instance methods. The one_to_one and one_to_many macros also make assumptions (just like ActiveRecord macros) about the database schema which may not be relevant in many cases.
  # 
  # === Caching model instances with memcached
  # 
  # Sequel models can be cached using memcached based on their primary keys. The use of memcached can significantly reduce database load by keeping model instances in memory. The set_cache method is used to specify caching:
  # 
  #   require 'memcache'
  #   CACHE = MemCache.new 'localhost:11211', :namespace => 'blog'
  # 
  #   class Author < Sequel::Model
  #     set_cache CACHE, :ttl => 3600
  #   end
  # 
  #   Author[333] # database hit
  #   Author[333] # cache hit
  # 
  # === Extending the underlying dataset
  # 
  # The obvious way to add table-wide logic is to define class methods to the model class definition. That way you can define subsets of the underlying dataset, change the ordering, or perform actions on multiple records:
  # 
  #   class Post < Sequel::Model
  #     def self.old_posts
  #       filter {:stamp < 30.days.ago}
  #     end
  # 
  #     def self.clean_old_posts
  #       old_posts.delete
  #     end
  #   end
  # 
  # You can also implement table-wide logic by defining methods on the dataset:
  # 
  #   class Post < Sequel::Model
  #     def dataset.old_posts
  #       filter {:stamp < 30.days.ago}
  #     end
  # 
  #     def dataset.clean_old_posts
  #       old_posts.delete
  #     end
  #   end
  # 
  # This is the recommended way of implementing table-wide operations, and allows you to have access to your model API from filtered datasets as well:
  # 
  #   Post.filter(:category => 'ruby').clean_old_posts
  # 
  # Sequel models also provide a short hand notation for filters:
  # 
  #   class Post < Sequel::Model
  #     subset(:old_posts) {:stamp < 30.days.ago}
  #     subset :invisible, :visible => false
  #   end
  # 
  # === Defining the underlying schema
  # 
  # Model classes can also be used as a place to define your table schema and control it. The schema DSL is exactly the same provided by Sequel::Schema::Generator:
  # 
  #   class Post < Sequel::Model
  #     set_schema do
  #       primary_key :id
  #       text :title
  #       text :category
  #       foreign_key :author_id, :table => :authors
  #     end
  #   end
  # 
  # You can then create the underlying table, drop it, or recreate it:
  # 
  #   Post.table_exists?
  #   Post.create_table
  #   Post.drop_table
  #   Post.create_table! # drops the table if it exists and then recreates it
  # 
  class Model
    alias_method :model, :class
  end

end

# TODO: add relationships when complete:
files = %w[
  base hooks record schema relations 
  caching plugins validations relationships
]
dir = File.join(File.dirname(__FILE__), "sequel_model")
files.each {|f| require(File.join(dir, f))}

module Sequel
  
  class Model
    
    # Defines a method that returns a filtered dataset.
    def self.subset(name, *args, &block)
      dataset.meta_def(name) {filter(*args, &block)}
    end
    
    # Finds a single record according to the supplied filter, e.g.:
    #
    #   Ticket.find :author => 'Sharon' # => record
    #   Ticket.find {:price == 17} # => Dataset
    #
    def self.find(*args, &block)
      dataset.filter(*args, &block).first
    end
    
    # TODO: doc
    def self.[](*args)
      args = args.first if (args.size == 1)
      if args === true || args === false
        raise Error::InvalidFilter, "Did you mean to supply a hash?"
      end
      dataset[(Hash === args) ? args : primary_key_hash(args)]
    end
    
    # TODO: doc
    def self.fetch(*args)
      db.fetch(*args).set_model(self)
    end

    # Like find but invokes create with given conditions when record does not
    # exists.
    def self.find_or_create(cond)
      find(cond) || create(cond)
    end

    ############################################################################

    # Deletes all records in the model's table.
    def self.delete_all
      dataset.delete
    end

    # Like delete_all, but invokes before_destroy and after_destroy hooks if used.
    def self.destroy_all
      dataset.destroy
    end
        
    def self.is_dataset_magic_method?(m)
      method_name = m.to_s
      Sequel::Dataset::MAGIC_METHODS.each_key do |r|
        return true if method_name =~ r
      end
      false
    end
    
    def self.method_missing(m, *args, &block) #:nodoc:
      Thread.exclusive do
        if dataset.respond_to?(m) || is_dataset_magic_method?(m)
          instance_eval("def #{m}(*args, &block); dataset.#{m}(*args, &block); end")
        end
      end
      respond_to?(m) ? send(m, *args, &block) : super(m, *args)
    end

    # TODO: Comprehensive description goes here!
    def self.join(*args)
      table_name = dataset.opts[:from].first
      dataset.join(*args).select(table_name.to_sym.ALL)
    end
    
    # Returns an array containing all of the models records.
    def self.all
      dataset.all
    end
  end

end
