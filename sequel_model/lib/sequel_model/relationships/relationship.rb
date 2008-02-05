module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   HasMany.new Post, :comments
    #   HasOne.new Post, :author, :class => "User"
    #   BelongsTo.new Comment, :post
    # @has_one = HasOne.new(Post, :author, :class => 'User').create
    class Relationship

      attr_reader :klass, :relation, :options, :join_table #, :arity

      def initialize(klass, relation, options = {})
        @klass    = klass
        @relation = relation
        @options  = options
        setup options
      end

      def setup(options = {})
        setup_join_table(options)
        define_relationship_accessor(options)
      end

      def setup_join_table(options = {})
        @join_table = JoinTable.new(self.klass.table_name, relation.to_s.pluralize, options)
        @join_table.send((@join_table.exists? && options[:force] == true) ? :create_table! : :create_table)
      end
      
      def relation_class
        Inflector.constantize(options[:class] ||= Inflector.classify(@relation))
      end
      
      def define_relationship_accessor(options = {})
        if arity == :one
          klass.class_eval "def #{@relation} ; #{reader(options[:type])} ; end"
        else
          klass.class_eval "def #{@relation} ; #{reader(options[:type])} ; end"
        end
      end
      
      private
      
      def reader(type = nil)
        [:embeded, :foreign].include?(type)  ? foreign_reader : join_reader
      end

      def foreign_reader
        "self.dataset.select(:#{relation.to_s.pluralize}.all)." <<
        "join(:#{join_table.name}, :#{@klass.to_s.foreign_key} => :id)." <<
        "join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key})." <<
        "filter(:#{klass.to_s.tableize}__id => self.id)"
      end

      def join_reader
        # The 'general' idea:
        #"self.dataset.select(:#{relation.to_s.pluralize}.all)" <<
        #"join(:#{join_table.name}, :#{table_name.to_s.singularize}_#{join_table.primary_key} => :#{primary_key})" <<
        #"join(:#{relation.to_s.pluralize}, :#{relation.primary_key} => :#{relation.to_s.pluralize}_#{relation.primary_key})" <<
        #"where(:#{table_name}__id => self.#{primary_key.to_s})"

        # TEMPORARY, getting the simple case working:
        "self.dataset.select(:#{relation.to_s.pluralize}.all)." <<
        "join(:#{join_table.name}, :#{@klass.to_s.foreign_key} => :id)." <<
        "join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key})." <<
        "filter(:#{klass.to_s.tableize}__id => self.id)"
      end
      
      def writer(type = nil)
        [:embeded, :foreign].include?(type)  ? foreign_writer : join_writer
      end
      
      # insert into foreign table
      # Post: has :one, :author
      # @post.author = @author
      #
      # Post: has :many, :comments
      # @post.comments << @comment        
      def embeded_writer
         "@source"
      end
      
      # insert into join table
      # eg CommentPost.create(key1,key2)
      def join_writer
        "@join_table.create(@source.id,@destination.id)"
      end
      
    end
    
      
  end

end
