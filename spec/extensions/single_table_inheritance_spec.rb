require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "#sti_key" do
  before do
    class ::StiTest < Sequel::Model
      def kind; self[:kind]; end 
      def kind=(x); self[:kind] = x; end 
      def _refresh(x); end 
      plugin :single_table_inheritance, :kind
    end 
    class ::StiTestSub1 < StiTest
    end 
    class ::StiTestSub2 < StiTest
    end 
    @ds = StiTest.dataset
    MODEL_DB.reset
  end
  after do
    Object.send(:remove_const, :StiTestSub1)
    Object.send(:remove_const, :StiTestSub2)
    Object.send(:remove_const, :StiTest)
  end

  specify "should have simple_table = nil" do
    StiTest.simple_table.should == nil
    StiTestSub1.simple_table.should == nil
  end
  
  it "should allow changing the inheritance column via a plugin :single_table_inheritance call" do
    StiTest.plugin :single_table_inheritance, :blah
    Object.send(:remove_const, :StiTestSub1)
    Object.send(:remove_const, :StiTestSub2)
    class ::StiTestSub1 < StiTest
    end 
    class ::StiTestSub2 < StiTest
    end 
    def @ds.fetch_rows(sql)
      yield({:blah=>'StiTest'})
      yield({:blah=>'StiTestSub1'})
      yield({:blah=>'StiTestSub2'})
    end 
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTestSub1, StiTestSub2]
    StiTest.dataset.sql.should == "SELECT * FROM sti_tests"
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.blah = 'StiTestSub1')"
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.blah = 'StiTestSub2')"
  end 
  
  it "should return rows with the correct class based on the polymorphic_key value" do
    def @ds.fetch_rows(sql)
      yield({:kind=>'StiTest'})
      yield({:kind=>'StiTestSub1'})
      yield({:kind=>'StiTestSub2'})
    end 
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTestSub1, StiTestSub2]
  end 

  it "should fallback to the main class if the given class does not exist" do
    def @ds.fetch_rows(sql)
      yield({:kind=>'StiTestSub3'})
    end
    StiTest.all.collect{|x| x.class}.should == [StiTest]
  end

  it "should fallback to the main class if the sti_key field is empty or nil without calling constantize" do
    called = false
    StiTest.meta_def(:constantize) do |s|
      called = true
      Object
    end
    StiTest.plugin :single_table_inheritance, :kind
    def @ds.fetch_rows(sql)
      yield({:kind=>''})
      yield({:kind=>nil})
    end
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTest]
    called.should == false
  end

  it "should add a before_create hook that sets the model class name for the key" do
    StiTest.new.save
    StiTestSub1.new.save
    StiTestSub2.new.save
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (kind) VALUES ('StiTest')", "INSERT INTO sti_tests (kind) VALUES ('StiTestSub1')", "INSERT INTO sti_tests (kind) VALUES ('StiTestSub2')"]
  end

  it "should have the before_create hook not override an existing value" do
    StiTest.create(:kind=>'StiTestSub1')
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (kind) VALUES ('StiTestSub1')"]
  end

  it "should add a filter to model datasets inside subclasses hook to only retreive objects with the matching key" do
    StiTest.dataset.sql.should == "SELECT * FROM sti_tests"
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind = 'StiTestSub1')"
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind = 'StiTestSub2')"
  end
end
