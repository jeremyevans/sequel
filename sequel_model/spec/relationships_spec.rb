require File.join(File.dirname(__FILE__), "spec_helper")

#__END__

# class Post < Sequel::Model
#   relationships do
#     has :one,  :blog, :required => true, :normalized => false # uses a blog_id field, which cannot be null, in the Post model
#     has :one,  :account # uses a join table called accounts_posts to link the post with it's account.
#     has :many, :comments # uses a comments_posts join table
#     has :many, :authors, :required => true  # authors_posts join table, requires at least one author
#   end
# end

class User < Sequel::Model; end

describe Sequel::Model, "relationships" do
  
  describe "has" do
    it "should allow for arity :one with options"
    it "should allow for arity :many with options"
    it "should raise an error Sequel::Error, \"Arity must be specified {:one, :many}.\" if arity was not specified."
  end
  
  describe "has_one" do
    it "should pass arguments to has :one" do
      User.should_receive(:has).with(:one, :boss, {}).and_return(true)
      User.send(:has_one, :boss)
    end
  end
  
  describe "has_many" do
    it "should pass arguments to has :many" do
      User.should_receive(:has).with(:many, :addresses, {}).and_return(true)
      User.send(:has_many, :addresses)
    end
  end

  describe "belongs_to" do
    it "should pass arguments to has :one" do
      @belongs_to_relationship = mock(Sequel::Model::BelongsToRelationship)
      @belongs_to_relationship.should_receive(:create)
      Sequel::Model::BelongsToRelationship.should_receive(:new).with(User, :boss, {}).and_return(@belongs_to_relationship)
      User.send(:belongs_to, :boss)
    end
  end
  
  describe "has_relationships?" do

  end

  describe "relationships block" do

    it "should store the relationship" do
      User.should_receive(:has).with(:one, :boss).and_return(true)
      class User
        relationships do
          has :one, :boss
        end
      end
      # User.model_relationships.should eql(?)
    end
    it "should create relationship methods on the model"
    it "should allow for has :one relationship"
    it "should allow for has :many relationship"
    it "should allow for has_one relationship"
    it "should allow for has_many relationship"
    it "should allow for belongs_to"
  end

end
