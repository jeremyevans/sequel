module Sequel
  class Model
    ID_POSTFIX = '_id'.freeze
    
    # Creates a 1-1 relationship by defining an association method, e.g.:
    # 
    #   class Session < Sequel::Model(:sessions)
    #   end
    #
    #   class Node < Sequel::Model(:nodes)
    #     one_to_one :producer, :from => Session
    #     # which is equivalent to
    #     def producer
    #       Session[producer_id] if producer_id
    #     end
    #   end
    #
    # You can also set the foreign key explicitly by including a :key option:
    #
    #   one_to_one :producer, :from => Session, :key => :producer_id
    #
    # The one_to_one macro also creates a setter, which accepts nil, a hash or
    # a model instance, e.g.:
    #
    #   p = Producer[1234]
    #   node = Node[:path => '/']
    #   node.producer = p
    #   node.producer_id #=> 1234
    #
    def self.one_to_one(name, opts)
      from = opts[:from]
      from || (raise Error, "No association source defined (use :from option)")
      key = opts[:key] || (name.to_s + ID_POSTFIX).to_sym
      
      setter_name = "#{name}=".to_sym
      
      case from
      when Symbol
        class_def(name) {(k = @values[key]) ? db[from][:id => k] : nil}
      when Sequel::Dataset
        class_def(name) {(k = @values[key]) ? from[:id => k] : nil}
      else
        class_def(name) {(k = @values[key]) ? from[k] : nil}
      end
      class_def(setter_name) do |v|
        case v
        when nil
          set(key => nil)
        when Sequel::Model
          set(key => v.pk)
        when Hash
          set(key => v[:id])
        end
      end

      # define_method name, &eval(ONE_TO_ONE_PROC % [key, from])
    end
  
    # Creates a 1-N relationship by defining an association method, e.g.:
    # 
    #   class Book < Sequel::Model(:books)
    #   end
    #
    #   class Author < Sequel::Model(:authors)
    #     one_to_many :books, :from => Book
    #     # which is equivalent to
    #     def books
    #       Book.filter(:author_id => id)
    #     end
    #   end
    #
    # You can also set the foreign key explicitly by including a :key option:
    #
    #   one_to_many :books, :from => Book, :key => :author_id
    #
    def self.one_to_many(name, opts)
      from = opts[:from]
      from || (raise Error, "No association source defined (use :from option)")
      key = opts[:key] || (self.to_s + ID_POSTFIX).to_sym
      
      case from
      when Symbol
        class_def(name) {db[from].filter(key => pk)}
      else
        class_def(name) {from.filter(key => pk)}
      end
    end
    
    # TODO: Add/Replace current relations with the following specifications:
    # ======================================================================
    
    # Database modelling is generally done with an ER (Entity Relationship) diagram.
    # Shouldn't ORM's facilitate simlilar specification?

    #   class Post < Sequel::Model(:users)
    #     relationships do
    #       # Specify the relationships that exist with the User model (users table)
    #       # These relationships are precisely the ER diagram connecting arrows.
    #     end
    #   end

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
    #   class Post < Sequel::Model(:users)
    #     relationships do
    #       has :one,  :blog, :required => true # blog_id field, cannot be null
    #       has :one,  :account # account_id field
    #       has :many, :comments # comments_posts join table
    #       has :many, :authors, :required => true  # authors_posts join table, requires at least one author
    #     end
    #   end

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
  end
end