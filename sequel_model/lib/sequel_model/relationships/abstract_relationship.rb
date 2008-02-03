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

      def create(options = {})
        create_join_table
        define_accessor(options)
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
      # DB[:posts].select(:comments.all).join(:comments_posts, :post_id => :id).join(:comments, :id => :comment_id).filter(:posts__id => 1).sql
      # => "SELECT comments.* FROM posts 
      # INNER JOIN comments_posts ON (comments_posts.`post_id` = posts.`id`) 
      # INNER JOIN comments ON (comments.`id` = comments_posts.`comment_id`) 
      # WHERE (posts.`id` = 1)"
      # klass.class_eval <<-ACCESSOR
      # def #{@relation}
      #   #{join_accessor}
      # end

      # def #{@relation}=(value)
      #   return false unless value
      #   self.dataset.set(:value => )
      # end
      # ACCESSOR
      
      def define_accessor(options = {})
        klass.class_eval <<-ACCESSOR
          def #{@relation}
            #{dataset_reader(options[:type])}
          end

          def #{@relation}=(value)
          end
        ACCESSOR
      end
      
      def dataset_reader(type)
        default_dataset_reader
      end

      def default_dataset_reader
        <<-QUERYBLOCK
        self.class.query do
          select(:#{relation.to_s.pluralize}.all)
          join(
          :#{join_table.name}, 
          :#{@klass.to_s.foreign_key} => :id
          )
          join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key})
          filter(:#{klass.to_s.tableize}__id => self.id)
        end
        QUERYBLOCK
      end

      def join_dataset_reader
        <<-QUERYBLOCK
        self.class.query do
          select(:#{relation.to_s.pluralize}.all)
          join(
          :#{join_table.name}, 
          :#{table_name.to_s.singularize}_#{join_table.primary_key} => :#{primary_key}
          )
          join(
          :#{relation.to_s.pluralize}, 
          :#{relation.primary_key} => :#{relation.to_s.pluralize}_#{relation.primary_key}
          )
          where(:#{table_name}__id => self.#{primary_key.to_s})
        end
        QUERYBLOCK
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
