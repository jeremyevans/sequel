require File.join(File.dirname(__FILE__), "../spec_helper")
#require File
describe Sequel::Model::JoinTable do
  before(:each) do
    @join_table = Sequel::Model::JoinTable.new :post, :comment
  end
  
  it "should have a proper join table name" do
    @join_table.name.should == 'comments_posts'
  end
  
  describe "exists?" do
    before :each do
      @db = mock("db instance")
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
end