module Sequel
  class Model
    # Manages relationships between to models
    # 
    #   HasOneRelationship.new Post, :one, :comments
    #   HasOneRelationship.new Post, :one, :author, :class => 'User'
    # @has_one = HasOneRelationship.new(Post, :one, :author, :class => 'User').create
    class AbstractRelationship

      attr_reader :klass, :relation, :options, :join_table

      def initialize(klass, relation, options)
        @klass = klass
        @relation = relation
        @options = options
      end

      def create(options = {})
        create_join_table
        define_accessor(options)
      end

      def create_join_table
        @join_table = JoinTable.new self.klass.table_name, relation.to_s.pluralize

        if @join_table.exists? && options[:force] == true
          @join_table.create!
        else
          @join_table.create
        end
      end

      def define_accessor(options)
        if options[:type] == :simple
          klass.class_eval <<-ACCESSOR
          def #{@relation}
            #{foreign_accessor}
          end

          def #{@relation}=(value)
          end
          ACCESSOR
        else
          klass.class_eval <<-ACCESSOR
          def #{@relation}
            #{join_accessor}
          end

          def #{@relation}=(value)
            return false unless value
            self.dataset.set(:value => )
          end
          ACCESSOR
        end

        def foreign_accessor
          <<-QUERYBLOCK
          self.query do
            select(:#{relation.to_s.pluralize}.all)
            join(
            :#{join_table.name}, 
            :#{@klass.to_s.foreign_key} => :id
            )
            join(:#{@relation.to_s.pluralize}, :id => :#{@relation.to_s.classify.foreign_key})
            )
            filter(:#{klass.to_s.tableize}__id => self.id)
          end
          QUERYBLOCK
        end

        def join_accessor
          <<-QUERYBLOCK
          self.query do
            select(:#{relation.to_s.pluralize}.all)
            join(
            :#{join_table.name}, 
            :#{Inflector.singularize(table_name)}_#{join_table.primary_key} => :#{primary_key}
            )
            join(
            :#{relation.to_s.pluralize}, 
            :#{relation.primary_key} => :#{relation.to_s.pluralize}_#{relation.primary_key}
            )
            where(:#{table_name}__id => self.#{primary_key.to_s})
          end
          QUERYBLOCK
        end

        def foreign_key
          @klass.to_s.foreign_key
        end 

        def relation_class
          Inflector.constantize(options[:class] ||= Inflector.classify(@relation))
        end

      end

      class HasOneRelationship < AbstractRelationship; end
      class HasManyRelationship < AbstractRelationship; end
      class BelongsToRelationship < HasOneRelationship; end
    end
  end
