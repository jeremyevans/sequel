require File.join(File.dirname(__FILE__), "../spec_helper")

describe Sequel::Model::AbstractRelationship do  
  describe "intance methods" do
    before :each do
      class Post < Sequel::Model(:posts); end
      class People < Sequel::Model(:people); end
      class Comment < Sequel::Model(:comment); end
      @one = Sequel::Model::HasOneRelationship.new Post, :author, {:class => "People"}
      @many = Sequel::Model::HasManyRelationship.new Post, :comments, {:force => true}
      @join_table = mock(Sequel::Model::JoinTable)
    end
    
    describe "create" do
      it "should call the create join table method" do
        @one.should_receive(:create_join_table).and_return(true)
        @one.should_receive(:define_accessor)
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
    
    describe "define_accessor" do      
      describe "reader" do
        it "should return an instance for a has :one relationship" do
          @one.should_receive(:join_table).and_return(@join_table)
          @join_table.should_receive(:name).and_return(:authors_posts)
          Post.should_receive(:class_eval).with(
"          def author
            self.dataset.join(:authors_posts, :post_id => :id, :id => self.id).join(:authors, :id => :author_id)
          end
          
          def author=(value)
          end
")
          @one.define_accessor
          #@one.should respond_to(:author)
        end
        
        it "should return all of the instances for a has :many relationship" do
          @many.should_receive(:join_table).and_return(@join_table)
          @join_table.should_receive(:name).and_return(:posts_comments)
          @many.define_accessor
          #@many.should respond_to(:comments)
        end
      end
      
      describe "writer" do
        it "should be created" do
        end
      end
    end
    
    describe "foreign_key" do
      before(:each) do
        @one.stub!(:create_join_table)
        @one.stub!(:define_accessor)
        @one.create
        @many.stub!(:create_join_table)
        @many.stub!(:define_accessor)
        @many.create
      end
      
      it "should give you the foreign key for the current class" do
        @one.foreign_key.should == "post_id"
        @many.foreign_key.should == "post_id"
      end
    end
  end
end