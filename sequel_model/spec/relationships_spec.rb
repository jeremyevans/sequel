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
describe Sequel::Model, "relationships" do
  before :all do
    class Smurf < Sequel::Model
    end
  end
  
  after :each do
    Smurf.model_relationships.clear
  end
  
  describe "has" do

  end
  
  describe "has_one" do

  end
  
  describe "has_many" do

  end

  describe "belongs_to" do

  end
  
  describe "has_relationships?" do

  end

end
