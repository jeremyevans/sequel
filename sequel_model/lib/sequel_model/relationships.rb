# = Sequel Relationships
# Database modelling is generally done with an ER (Entity Relationship) diagram.
# Shouldn't ORM's facilitate simlilar specification?

# class Post < Sequel::Model
#   relationships do
#     # Specify the relationships that exist with the User model (users table)
#     # These relationships are precisely the ER diagram connecting arrows.
#   end
# end

#
# = Relationships 
#
# are specifications of the ends of the ER diagrams connectors that are touching
# the current model.
#
# one_to_one,   has_one
# many_to_one,  belongs_to
# many_to_many, has_many

# ?parameters may be :zero, :one, :many which specifies the cardinality of the connection

# Example:
# class Post < Sequel::Model
#   relationships do
#     has :one,  :blog, :required => true, :normalized => false # uses a blog_id field, which cannot be null, in the Post model
#     has :one,  :account # uses a join table called accounts_posts to link the post with it's account.
#     has :many, :comments # uses a comments_posts join table
#     has :many, :authors, :required => true  # authors_posts join table, requires at least one author
#   end
# end
# 
#
# Relationship API Details
#

#
# == belongs_to
#

# Defines an blog and blog= method
#   belongs_to :blog

# Same, but uses "b_id" as the blog's id field.
#   belongs_to :blog, :key => :b_id

#   has_many   :comments
# * Defines comments method which will query the join table appropriately.
# * Checks to see if a "comments_posts" join table exists (alphabetical order)
# ** If it does not exist, will create the join table. 
# ** If options are passed in these will be used to further define the join table.


# Benefits:
# * Normalized DB
# * Easy to define join objects
# * Efficient queries, database gets to use indexed fields (pkeys) instead of a string field and an id.
# 
# For example, polymorphic associations now become:
# [user]      1-* [addresses_users]     *-1 [addresses]
# [companies] 1-* [addresses_companies] *-1 [addresses]
# [clients]   1-* [addresses_clients]   *-1 [addresses]
# it is automatically polymorphic by specifying the has relationship inside the 2User and Company tables to addresses. Addresses themselves don't care. so we have by default polymorphism. 
# If you need to talk about a 'Company Address' then you can subclass, CompanyAddress < Address and do has :many, :company_addresses

module Sequel

  class Model

    module Relationships
      class Generator
        def initialize(model_class, &block)
          @model_class = model_class
          instance_eval(&block)
        end

        def method_missing(method, *args)
          @model_class.send(method, *args)
        end
      end
    end

    class << self
      cattr_reader :model_relationships
      @@model_relationships = []

      def relationship_exists?(arity,relation)
        @@model_relationships.detect do |relation|
          relation[:arity] == arity && 
          relation[:klass] == relation
        end
      end

      # has arity<Symbol>, model<Symbol>
      # has :one,  :blog, :required => true # blog_id field, cannot be null
      # has :one,  :account # account_id field
      # has :many, :comments # comments_posts join table
      def has(arity, klass, options = {})

        # Commence with the sanity checks!
        unless [:one,:many].include? arity
          raise Sequel::Error, "Arity must be specified {:one, :many}." 
        end

        if relationship_exists?(arity, klass)
          raise Sequel::Error, "The relationship #{self.class.name} has #{arity}, #{klass} is already defined."
        end

        #unless const_defined?(klass.to_s.camel_case)
          #raise Sequel::Error, "#{klass.to_s.camel_case} does not exist"
        #end

        # Make sure the join table exists
        auto_create_join_table(klass, options)

        # Store the relationship
        @@model_relationships << { 
          :arity => arity, 
          :klass => klass, 
          :options => options 
        }

        # Define relationship methods
        after_initialize do
          define_relationship_method arity, klass, options
        end

        #unless normalized
          # :required => true # The relationship must be populated to save
          # can only be used with normalized => false : 
        #end
        # save the relationship
      end
      
      # the proxy methods has_xxx ... , simply pass thru to to has :xxx, ...
      def has_one(klass, options = {})
        has :one, klass, options
      end

      def has_many(klass, options = {})
        has :many, klass, options
      end

      def belongs_to(klass, options = {})
        has :one, klass, options
      end

      # returns true if exists, false if not
      def join_table?(first, second)
        # we still have to test this out      
        db[join_table(first, second)].table_exists?
      end

      # TODO: Move this elsewhere? outside relationships?
      # creates a join table given two table names
      def create_join_table(first, second)
        first_key, second_key = "#{first}_id", "#{second}_id"
        db.create_table join_table(first, second).to_sym do
          #primary_key [first_key.to_sym, second_key.to_sym]
          integer first_key, :null => false
          integer second_key, :null => false
        end unless join_table?(first, second)
      end

      def create_join_table!(first, second)
        db.drop_table join_table(first, second)
        create_join_table(first, second)
      end

      def auto_create_join_table!(klass)
        auto_create_join_table(klass, :force => true)
      end

      def auto_create_join_table(klass, options = {})
        first, second = table_name, klass.to_s.pluralize
        if join_table?(first, second) && options[:force] == true
          create_join_table!(first, second)
        else
          create_join_table(first, second)
        end
      end

      # Given two models, it outputs the join table name
      # which is sorted alphabetically with each table name pluralized
      # Examples:
      #   join_table(user, post) #=> :posts_users
      #   join_table(users, posts) #=> :posts_users
      def join_table(first, second)
        first, second = first.to_s.pluralize, second.to_s.pluralize
        [first, second].sort.join("_").to_sym
      end

      # relationships do
      #   ...
      # end
      def relationships(&block)
        Relationships::Generator.new(self, &block)
      end

      # return true if there are validations stored, false otherwise
      def has_relationships?
        model_relationships.length > 0 ? true : false
      end

      # TODO: figure out what we want to do with these...
      # "FooBar".snake_case #=> "foo_bar"
      def snake_case
        gsub(/\B[A-Z]/, '_\&').downcase
      end

      # "foo_bar".camel_case #=> "FooBar"
      def camel_case
        split('_').map{|e| e.capitalize}.join
      end
    end

    # Defines relationship method from the current class to the klass specified
    def define_relationship_method(arity, relation, options)
      if arity == :one
        self.instance_eval "
          def #{relation}
            self.db.dataset.left_outer_join(#{relation}, :id => :#{relation.to_s.singularize}_id).limit(1)
          end
        "
      elsif arity == :many
        self.instance_eval "
          def #{relation}
            self.db.dataset.left_outer_join(#{relation}, :id => :#{relation.to_s.singularize}_id)
          end
        "
      end
    end # define_relationship_method
  end # Model
end # Sequel
