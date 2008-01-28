module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   HasOneRelationship.new Post, :one, :comments    
    class AbstractRelationship
      
      attr_reader :klass, :arity, :relation, :options
      
      def initialize(klass, arity, relation, options)
        @klass = klass
        @arity = arity
        @relation = relation
        @options = options
      end
      
      def create
        create_join_table
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
      def define_relationship_accessor
        klass.class_eval <<-EOS
          def #{@relation}
            #self.dataset.left_outer_join(#{@relation}, :id => :#{self.primary_key_string}).limit(1)
            puts #{relation_class}
          end
          
          def #{@relation}=(value)
          end
        EOS
      end
      
      def relation_class
        Inflector.constantize(Inflector.classify(@relation))
      end
      
    end
    
    class HasOneRelationship < AbstractRelationship; end
    class HasManyRelationship < AbstractRelationship; end
    class BelognsToRelationship < HasOneRelationship; end
  end
end