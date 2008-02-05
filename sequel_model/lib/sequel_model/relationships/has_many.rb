module Sequel
  class Model
    class HasMany < Relationship

      def arity ; :many ; end

      # Post.comments.create(:body => "")
      def create(*args)
        self.<< @destination.create(*args)
      end

      # Post.comments << @comment
      # inserts the class into the join table
      # sets the other's foreign key field if options[:simple]
      def <<(other)
        other.save if other.new?
        # add the other object to the relationship set by inserting into the join table
      end

      def define_relationship_accessor(options = {})
        klass.class_eval "def #{@relation} ; #{reader(options[:type])} ; end"
      end

    end
  end
end