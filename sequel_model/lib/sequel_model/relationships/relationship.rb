module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   Relationship.new Post, :one, :comments
    class Relationship < AbstractRelationship
      def create
        create_join_table
      end

      def create_join_table
        join_table = JoinTable.new klass.table_name, relation.to_s.pluralize
        
        if join_table.exists? && options[:force] == true
          join_table.create!
        else
          join_table.create
        end
      end
    end
  end
end