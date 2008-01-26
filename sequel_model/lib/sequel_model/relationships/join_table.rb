module Sequel
  class Model
    
    # Handles join tables.
    # Parameters are the first class and second class:
    #
    #   @join_table = JoinTable.new :post, :comment
    #
    # The join table class object is available via
    #   @join_table.class #=> PostComment
    class JoinTable
      
      attr_accessor :join_class
      
      def self.key(klass)
        [ Inflector.singularize(klass.table_name), klass.primary_key_string) ].join("_")
      end
      
      def initialize(first_klass, second_klass)
        @first_klass  = first_klass
        @second_klass = second_klass
        
        # Automatically Define the JoinClass if it does not exist
        instance_eval <<-JOINCLASS
        unless defined?(::#{@first_klass.class}#{@first_klass.class})
          @class = class ::#{@first_klass.class}#{@first_klass.class} < Sequel::Model ; end
          @class.set_primary_key :#{key(@first_klass)}, :#{key(@second_klass)}
        else
          @class = ::#{@first_klass.class}#{@first_klass.class}
        end
        JOINCLASS
      end
      
      # Outputs the join table name
      # which is sorted alphabetically with each table name pluralized
      # Examples:
      #   join_table(user, post) #=> :posts_users
      #   join_table(users, posts) #=> :posts_users
      def name
        [ @first_klass.table_name, @second_klass.table_name ].sort.join("_")
      end
      
      # creates a join table
      def create
        if !exists?
          db.create_table name.to_sym do
            integer self.class.key(@first_klass),  :null => false
            integer self.class.key(@second_klass), :null => false
          end
          
          true
        else
          false
        end
      end
      
      # drops the the table if it exists and creates a new one
      def create!
        db.drop_table name if exists?
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
