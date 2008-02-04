module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   HasOneRelationship.new Post, :one, :comments
    #   HasOneRelationship.new Post, :one, :author, :class => 'User'
    # @has_one = HasOneRelationship.new(Post, :author, :class => 'User').create
    class AbstractRelationship

      attr_reader :klass, :relation, :options, :join_table #, :arity

      def initialize(klass, relation, options = {})
        @klass    = klass
        @relation = relation
        @options  = options
      end

      def create(options = {})
        create_join_table
        define_relationship_accessor(options)
      end

      def create_join_table
        @join_table = JoinTable.new self.klass.table_name, relation.to_s.pluralize
        @join_table.send((@join_table.exists? && options[:force] == true) ? :create! : :create)
      end
      
      def relation_class
        Inflector.constantize(options[:class] ||= Inflector.classify(@relation))
      end
      
      def define_relationship_accessor(options = {})
        if arity == :one
          klass.class_eval "def #{@relation} ; #{relationship_reader(options[:type])} ; end"
          klass.class_eval "def #{@relation}=(value) ; #{relationship_writer(options[:type])} ; end"
        else
          klass.class_eval "def #{@relation} ; #{relationship_reader(options[:type])} ; end"
          # klass.class_eval "def #{@relation}<<(value) ; #{relationship_writer(options[:type])} ; end"
        end
      end
      
      private
      
      def relationship_reader(type = nil)
        if [:embeded, :foreign].include?(type) 
          embeded_relationship_reader
        else
          join_relationship_reader
        end
      end

      def embeded_relationship_reader
        <<-QUERYBLOCK
        self.dataset.
          select(:#{relation.to_s.pluralize}.all).
          join(
          :#{join_table.name}, 
          :#{@klass.to_s.foreign_key} => :id
          ).
          join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key}).
          filter(:#{klass.to_s.tableize}__id => self.id)
        QUERYBLOCK
      end

      def join_relationship_reader
        #<<-QUERYBLOCK
        #self.class.query do
        #  select(:#{relation.to_s.pluralize}.all)
        #  join(
        #  :#{join_table.name}, 
        #  :#{table_name.to_s.singularize}_#{join_table.primary_key} => :#{primary_key}
        #  )
        #  join(
        #  :#{relation.to_s.pluralize}, 
        #  :#{relation.primary_key} => :#{relation.to_s.pluralize}_#{relation.primary_key}
        #  )
        #  where(:#{table_name}__id => self.#{primary_key.to_s})
        #end
        #QUERYBLOCK

        # TEMPORARY, getting the simple case working
        <<-QUERYBLOCK
        self.class.
          select(:#{relation.to_s.pluralize}.all).
          join(
          :#{join_table.name}, 
          :#{@klass.to_s.foreign_key} => :id
          ).
          join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key}).
          filter(:#{klass.to_s.tableize}__id => self.id)
        QUERYBLOCK
      end
      
      def relationship_writer(type = nil)
        if [:embeded, :foreign].include?(type) 
          embeded_relationship_writer
        else
          join_relationship_writer
        end
      end
      
      def embeded_relationship_writer
        # insert into foreign table
        # Post: has :one, :author
        # @post.author = @author
        #
        # Post: has :many, :comments
        # equals does not make sense however defining << would make sense so:
        # @post.comments << @comment        
         <<-QUERYBLOCK
            
          QUERYBLOCK
      end
      
      def join_relationship_writer
        # insert into join table
        # eg CommentPost.create(key1,key2)
        <<-QUERYBLOCK
           @join_table.create(@source.id,@destination.id)
         QUERYBLOCK
      end
      
    end
      
    class HasOneRelationship    < AbstractRelationship
      def arity ; :one ; end
    end

    class HasManyRelationship   < AbstractRelationship
      def arity ; :many ; end
    end

    class BelongsToRelationship < HasOneRelationship ; end
      
  end
end
