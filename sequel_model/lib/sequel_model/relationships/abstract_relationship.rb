module Sequel
  class Model
    class AbstractRelationship
      
      attr_reader :klass, :arity, :relation, :options
      
      def initialize(klass, arity, relation, options)
        @klass = klass
        @arity = arity
        @relation = relation
        @options = options
      end
      
      def create
      end
      
      # SELECT c.* FROM comments c, comments_posts cp, posts p where c.id = cp.comment_id and cp.post_id = p.id and p.id = ?
      def define_relationship_accessor
        klass.class_eval <<-EOS
          def #{@relation}
            #self.dataset.left_outer_join(#{@relation}, :id => :#{self.class.table_name.to_s.singularize}_id).limit(1)
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
  end
end