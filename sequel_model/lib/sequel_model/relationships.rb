files = %w{ scoping relationship block join_table }
dir = File.join(File.dirname(__FILE__), "relationships")
files.each {|f| require(File.join(dir, f))}

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
    
    class << self
      @relationships = []
      
      # has arity<Symbol>, model<Symbol>
      # has :one,  :blog, :required => true # blog_id field, cannot be null
      # has :one,  :account # account_id field
      # has :many, :comments # comments_posts join table
      # has :many, :comment # comments_posts join table
      def has(arity, relation, options = {})
        # Create and store the relationship
        case arity
        when :one
          @relationships << HasOne.new(self, relation, options)
        when :many
          @relationships << HasMany.new(self, relation, options)
        else
          raise Sequel::Error, "Arity must be specified {:one, :many}."
        end
        
        #unless normalized
          # :required => true # The relationship must be populated to save
          # can only be used with normalized => false : 
        #end
        # save the relationship
      end
      
      # the proxy methods has_xxx ... , simply pass thru to to has :xxx, ...
      def has_one(relation, options = {})
        has :one, relation, options
      end

      def has_many(relation, options = {})
        has :many, relation, options
      end

      def belongs_to(relation, options = {})
        @relationships << BelongsTo.new(self, relation, options)
      end
      
      #def primary_key_string
      #  "#{self.to_s.tableize.singularize}_id"
      #end

    end
    
  end # Model

end # Sequel
