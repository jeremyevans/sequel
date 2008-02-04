require File.join(File.dirname(__FILE__), "../spec_helper")

describe Sequel::Model::AbstractRelationship do

  describe "intance methods" do

    before :each do
      class Post < Sequel::Model(:posts); end
      class People < Sequel::Model(:people); end
      class Comment < Sequel::Model(:comments); end
      @one = Sequel::Model::HasOneRelationship.new Post, :author, {:class => "People"}
      @many = Sequel::Model::HasManyRelationship.new Post, :comments, {:force => true}
      @belong = Sequel::Model::BelongsToRelationship.new Post, :author, {:class => "People"}
      @join_table = mock(Sequel::Model::JoinTable)
    end
    
    describe "create" do
      it "should call the create join table method" do
        @one.should_receive(:create_join_table).and_return(true)
        @one.should_receive(:define_relationship_accessor)
        @one.create
      end
    end
    
    describe "create_join_table" do
      before :each do
        @one.stub!(:define_accessor)
        @many.stub!(:define_accessor)
      end
      
      it "should create the table if it doesn't exist" do
        Post.should_receive(:table_name).and_return('posts')
        Sequel::Model::JoinTable.should_receive(:new).with('posts', 'authors').and_return(@join_table)
        @join_table.should_receive(:exists?).and_return(false)
        @join_table.should_receive(:create)
        @one.create_join_table
        @one.join_table.should == @join_table
      end
      
      it "should force create the table when the option is specified" do
        Post.should_receive(:table_name).and_return('posts')
        Sequel::Model::JoinTable.should_receive(:new).with('posts', 'comments').and_return(@join_table)
        @join_table.should_receive(:exists?).and_return(true)
        @join_table.should_receive(:create!)
        @many.create_join_table
        @many.join_table.should == @join_table
      end
    end
    
    describe "define_relationship_accessor" do      

      describe "reader" do        

        it "should return a dataset for a has :one relationship" do
          @one.stub!(:create_table)
          @one.should_receive(:join_table).and_return(@join_table)
          @join_table.should_receive(:name).and_return(:authors_posts)
          @one.define_relationship_accessor
          @post = Post.new(:id => 1)
          @post.author.sql.should == "SELECT authors.* FROM posts INNER JOIN authors_posts ON (authors_posts.post_id = posts.id) INNER JOIN authors ON (authors.id = authors_posts.author_id) WHERE (posts.id = #{@post.id})"
        end
        
        it "should return a dataset for a has :many relationship" do
          @many.should_receive(:join_table).and_return(@join_table)
          @join_table.should_receive(:name).and_return(:posts_comments)
          @many.define_relationship_accessor
          @post = Post.new(:id => 1)
          @post.comments.sql.should == "SELECT comments.* FROM posts INNER JOIN posts_comments ON (posts_comments.post_id = posts.id) INNER JOIN comments ON (comments.id = posts_comments.comment_id) WHERE (posts.id = #{@post.id})"
        end

      end
      
      describe "writer" do

        it "should define a writer method 'relation=' for the relation model when the relationship is has :one" do
          
        end

        it "should define the append method '<<' for the relation model when the relationship is has :many" do
          
        end

      end

    end
    
    describe "arity" do
      it "should return :one when an instance of HasOneRelationship" do
        @one.arity.should == :one
      end
      
      it "should return :many when an instance of HasManyRelationship" do
        @many.arity.should == :many
      end
      
      it "should return :one when an instance of BelongsToRelationship" do
        @belong.arity.should == :one
      end
    end
    
  end
end
