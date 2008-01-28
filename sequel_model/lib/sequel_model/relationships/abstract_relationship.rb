module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   HasOneRelationship.new Post, :one, :comments
    #   HasOneRelationship.new Post, :one, :author, :class => 'User'
    class AbstractRelationship
      
      attr_reader :klass, :relation, :options
      
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
        join_table = JoinTable.new self.klass.table_name, relation.to_s.pluralize
        
        if join_table.exists? && options[:force] == true
          join_table.create!
        else
          join_table.create
        end
      end
      
      # SELECT c.* FROM comments c, comments_posts cp, posts p where c.id = cp.comment_id and cp.post_id = p.id and p.id = ?
      def define_accessor
        klass.class_eval <<-ACCESSOR
          def #{@relation}
            #self.dataset.left_outer_join(#{@relation}, :id => :#{klass.primary_key_string}).limit(1)
            puts #{relation_class}
          end
          
          def #{@relation}=(value)
          end
        ACCESSOR
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