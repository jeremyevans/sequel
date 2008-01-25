module Sequel
  class Model
    
    # Handles join tables.
    # Parameters are the first class and second class:
    #
    #   JoinTable.new :post, :comment
    class JoinTable
      
      def self.key(klass)
        "#{klass}_id"
      end
      
      def initialize(first_klass, second_klass)
        @first_klass = first_klass
        @second_klass = second_klass
      end
      
      # Outputs the join table name
      # which is sorted alphabetically with each table name pluralized
      # Examples:
      #   join_table(user, post) #=> :posts_users
      #   join_table(users, posts) #=> :posts_users
      def name
        first_klass, second_klass = @first_klass.to_s.pluralize, @second_klass.to_s.pluralize
        [first_klass, second_klass].sort.join("_")
      end
      
      # creates a join table
      def create
        if !exists?
          db.create_table name.to_sym do
            #primary_key [first_key.to_sym, second_key.to_sym]
            integer self.class.key(@first_klass), :null => false
            integer self.class.key(@second_klass), :null => false
          end
          
          true
        else
          false
        end
      end
      
      # drops the the table if it exists and creates a new one
      def create!
        db.drop_table name
        create
      end
      
      # returns true if exists, false if not
      def exists?   
        db[name].table_exists?
      end
      
      def db
        Inflector.constantize(Inflector.classify(@first_klass)).db
      end
      
    end

  end
end