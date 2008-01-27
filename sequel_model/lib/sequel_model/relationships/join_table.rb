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
      attr_accessor :source
      attr_accessor :destination
      
      def self.key(klass)
        [ Inflector.singularize(klass.table_name), klass.primary_key_string.to_s ].join("_")
      end
      
      def initialize(source, destination)
        @source  = Inflector.constantize(Inflector.classify(source))
        @destination = Inflector.constantize(Inflector.classify(destination))
        
        # Automatically Define the JoinClass if it does not exist
        instance_eval <<-JOINCLASS
        unless defined?(::#{@source}#{@destination})
          @join_class = 
          class ::#{@source}#{@destination} < Sequel::Model
            set_primary_key :#{self.class.key(@source)}, :#{self.class.key(@destination)}
          end
        else
          @join_class = ::#{@source}#{@destination}
        end
        JOINCLASS
      end
      
      # Outputs the join table name
      # which is sorted alphabetically with each table name pluralized
      # Examples:
      #   join_table(user, post) #=> :posts_users
      #   join_table(users, posts) #=> :posts_users
      def name
        [source.table_name.to_s, destination.table_name.to_s].sort.join("_")
      end
      
      # creates a join table
      def create
        if !exists?
          db.create_table name.to_sym do
            integer self.class.key(@source),  :null => false
            integer self.class.key(@destination), :null => false
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
        @source.db
      end
      
    end

  end
end
