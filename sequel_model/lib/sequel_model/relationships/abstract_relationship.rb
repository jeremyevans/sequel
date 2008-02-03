module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   HasOneRelationship.new Post, :one, :comments
    #   HasOneRelationship.new Post, :one, :author, :class => 'User'
    # @has_one = HasOneRelationship.new(Post, :one, :author, :class => 'User').create
    class AbstractRelationship
      
      attr_reader :klass, :relation, :options, :join_table
      
      def initialize(klass, relation, options)
        @klass = klass
        @relation = relation
        @options = options
      end
      
      def create
        create_join_table
        define_accessor
      end

      def create_join_table
        @join_table = JoinTable.new self.klass.table_name, relation.to_s.pluralize
        
        if @join_table.exists? && options[:force] == true
          @join_table.create!
        else
          @join_table.create
        end
      end
      
      # SELECT c.* FROM comments c, comments_posts cp, posts p where c.id = cp.comment_id and cp.post_id = p.id and p.id = ?
      # @post.comments
      # SELECT posts.*, comments.*
      # FROM posts LEFT OUTER JOIN comments_posts LEFT OUTER JOIN comments
      # ON posts.pk = comments_posts.posts_pk, comments_posts.comments_pk = comments.pk
      # WHERE where_clause if given
      # LIMIT limit if given
      # ORDER order if given
      # DB[:posts].join(:comments_posts, :post_id => :id, :id => 1).join(:comments, :id => :comment_id).sql
      # => "SELECT * FROM posts 
      # INNER JOIN comments_posts ON (comments_posts.`post_id` = posts.`id`) AND (comments_posts.`id` = 1) 
      # INNER JOIN comments ON (comments.`id` = comments_posts.`comment_id`)"
      def define_accessor
        klass.class_eval <<-ACCESSOR
          def #{@relation}
            self.dataset.join(:#{join_table.name}, :#{foreign_key} => :id, :id => self.id).join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key})
          end
          
          def #{@relation}=(value)
          end
        ACCESSOR
      end
      
      def foreign_key
        @klass.to_s.foreign_key
      end 
      
      def relation_class
        Inflector.constantize(options[:class] ||= Inflector.classify(@relation))
      end
      
    end
    
    class HasOneRelationship < AbstractRelationship; end
    class HasManyRelationship < AbstractRelationship; end
    class BelongsToRelationship < HasOneRelationship; end
  end
end