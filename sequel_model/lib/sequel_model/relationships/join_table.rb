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
      
      def self.keys(klass)
        singular_klass = Inflector.singularize(klass.table_name)        
        [klass.primary_key].flatten.map do |key|
          [ singular_klass, key.to_s ].join("_")
        end
      end
      
      def initialize(source, destination)
        @source      = Inflector.constantize(Inflector.classify(source))
        @destination = Inflector.constantize(Inflector.classify(destination))

        # Automatically Define the JoinClass if it does not exist
        instance_eval <<-JOINCLASS
        unless defined?(::#{@source}#{@destination})
          @join_class = 
          class ::#{@source}#{@destination} < Sequel::Model
            set_primary_key [:#{(self.class.keys(@source) + self.class.keys(@destination)).join(", :")}]
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
        [@source.table_name.to_s, @destination.table_name.to_s].sort.join("_")
      end
      
      # creates a join table
      def create
        if !exists?
          # tablename_key1, tablename_key2,...
          # TODO: Inflect!, define a method to return primary_key as an array
          create_code =  <<-JOINTABLE
          db.create_table name.to_sym do
            #{@source.primary_key_def.reverse.join(" :#{Inflector.singularize(@source.table_name)}_")}, :null => false
            #{@destination.primary_key_def.reverse.join(" :#{Inflector.singularize(@destination.table_name)}_")}, :null => false
          end
          JOINTABLE
          instance_eval create_code
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
        self.db[name.to_sym].table_exists?
      end
      
      def db
        @source.db
      end
      
    end

  end
end
