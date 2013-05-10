require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "null_dataset extension" do
  before do
    @db = Sequel::mock(:fetch=>{:id=>1}, :autoid=>1, :numrows=>1, :columns=>[:id]).extension(:null_dataset)
    @ds = @db[:table].nullify
    @i = 0
    @pr = proc{|*a| @i += 1}
  end
  after do
    @db.sqls.should == [] unless @skip_check
  end

  it "should make each be a noop" do
    @ds.each(&@pr).should equal(@ds)
    @i.should == 0
  end

  it "should make fetch_rows be a noop" do
    @ds.fetch_rows("SELECT 1", &@pr).should == nil
    @i.should == 0
  end

  it "should make insert be a noop" do
    @ds.insert(1).should == nil
  end

  it "should make update be a noop" do
    @ds.update(:a=>1).should == 0
  end

  it "should make delete be a noop" do
    @ds.delete.should == 0
  end

  it "should make truncate be a noop" do
    @ds.truncate.should == nil
  end

  it "should make execute_* be a noop" do
    @ds.send(:execute_ddl,'FOO').should == nil
    @ds.send(:execute_insert,'FOO').should == nil
    @ds.send(:execute_dui,'FOO').should == nil
    @ds.send(:execute,'FOO').should == nil
  end

  it "should have working columns" do
    @skip_check = true
    @ds.columns.should == [:id]
    @db.sqls.should == ['SELECT * FROM table LIMIT 1']
  end

  it "should have count return 0" do
    @ds.count.should == 0
  end

  it "should have empty return true" do
    @ds.empty?.should == true
  end

  it "should make import a noop" do
    @ds.import([:id], [[1], [2], [3]]).should == nil
  end

  it "should have nullify method returned modified receiver" do
    @skip_check = true
    ds = @db[:table]
    ds.nullify.should_not equal(ds)
    ds.each(&@pr)
    @db.sqls.should == ['SELECT * FROM table']
    @i.should == 1
  end

  it "should have nullify! method modify receiver" do
    ds = @db[:table]
    ds.nullify!.should equal(ds)
    ds.each(&@pr)
    @i.should == 0
  end

  it "should work with method chaining" do
    @ds.where(:a=>1).select(:b).each(&@pr)
    @i.should == 0
  end
end
