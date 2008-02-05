module Sequel
  class Model
    class HasOne < Relationship

      def arity ; :one ; end

      # Post.author = @author
      def set(other)
        other.save if other.new?
        unless options[:type] == :simple
          # store in foreign key of other table
        else
          # store in join table
        end
      end

      def define_relationship_accessor(options = {})
        klass.class_eval "def #{@relation} ; #{reader(options[:type])} ; end"
        klass.class_eval "def #{@relation}=(value) ; #{writer(options[:type])} ; end"
      end

    end

    class BelongsTo < HasOne ; end
  end
end