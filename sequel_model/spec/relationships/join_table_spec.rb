require File.join(File.dirname(__FILE__), "../spec_helper")

describe Sequel::Model::JoinTable do  
  describe "class methods" do
    it "should have key method" do
      Sequel::Model::JoinTable.key(:post).should == 'post_id'
    end
  end
  
  describe "instance methods" do
    before(:each) do
      @join_table = Sequel::Model::JoinTable.new :post, :comment
      @join_table_plural = Sequel::Model::JoinTable.new :posts, :comments
      @join_table_string = Sequel::Model::JoinTable.new 'posts', 'comments'
      @db = mock('db instance')
    end
    
    describe "name" do
      it "should have a proper join table name" do
        @join_table.name.should == 'comments_posts'
        @join_table_plural.name.should == 'comments_posts'
        @join_table_string.name.should == 'comments_posts'
      end
    end
  
    describe "exists?" do
      before :each do
        @join_table.should_receive(:db).and_return(@db)
        @db.should_receive(:[]).with('comments_posts').and_return(@db)
      end
    
      it "should indicate if the table exists" do
        @db.should_receive(:table_exists?).and_return(true)
        @join_table.exists?.should == true
      end
  
      it "should indicate if the table does not exist" do
        @db.should_receive(:table_exists?).and_return(false)
        @join_table.exists?.should == false
      end
    end
    
    describe "create" do
      it "should create the table if it doesn't exist" do
        @join_table.should_receive(:exists?).and_return(false)
        @join_table.should_receive(:db).and_return(@db)
        @db.should_receive(:create_table).with(:comments_posts)
        @join_table.create.should be_true
      end
    
      it "should fail to create the table if it does exist" do
        @join_table.should_receive(:exists?).and_return(true)
        @join_table.create.should be_false
      end
    end
    
    describe "create!" do
      it "should allow you to force the creation of the table it does exist" do
        @join_table.should_receive(:db).and_return(@db)
        @db.should_receive(:drop_table).with('comments_posts')
        @join_table.should_receive(:create).and_return(true)
        @join_table.create!.should be_true
      end
    end
    
    describe "db" do
      it "should have access to the db object" do
        class Post; end
      
        Post.should_receive(:db).and_return(@db)
        @join_table.db.should == @db
      end
    end
  end
end