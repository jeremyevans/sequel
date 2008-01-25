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
      
      def define_relationship_accessor(klass)
        klass.class_eval <<-EOS
          def #{@relation}
            #self.dataset.left_outer_join(#{@relation}, :id => :#{self.class.table_name.to_s.singularize}_id).limit(1)
          end
          
          def #{@relation}=(value)
          end
        EOS
      end
      
    end
  end
end