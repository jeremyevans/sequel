require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

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
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.blah IN ('StiTestSub1'))"
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.blah IN ('StiTestSub2'))"
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

  it "should have the before_create hook handle columns with the same name as existing method names" do
    StiTest.plugin :single_table_inheritance, :type
    StiTest.columns :id, :type
    StiTest.create
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (type) VALUES ('StiTest')"]
  end

  it "should add a filter to model datasets inside subclasses hook to only retreive objects with the matching key" do
    StiTest.dataset.sql.should == "SELECT * FROM sti_tests"
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub1'))"
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub2'))"
  end

  it "should add a correct filter for multiple levels of subclasses" do
    class ::StiTestSub1A < StiTestSub1; end
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub1', 'StiTestSub1A'))"
    StiTestSub1A.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub1A'))"
    class ::StiTestSub2A < StiTestSub2; end
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub2', 'StiTestSub2A'))"
    StiTestSub2A.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub2A'))"
    class ::StiTestSub1B < StiTestSub1A; end
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub1', 'StiTestSub1A', 'StiTestSub1B'))"
    StiTestSub1A.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub1A', 'StiTestSub1B'))"
    StiTestSub1B.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.kind IN ('StiTestSub1B'))"
  end

  describe "with custom options" do
    before do
      class ::StiTest2 < Sequel::Model
        columns :id, :kind
        def _refresh(x); end
      end
    end
    after do
      Object.send(:remove_const, :StiTest2)
      Object.send(:remove_const, :StiTest3)
      Object.send(:remove_const, :StiTest4)
    end

    it "should work with custom procs with strings" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>proc{|v| v == 1 ? 'StiTest3' : 'StiTest4'}, :key_map=>proc{|klass| klass.name == 'StiTest3' ? 1 : 2}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
      StiTest2.dataset.row_proc.call(:kind=>0).should be_a_instance_of(StiTest4)
      StiTest2.dataset.row_proc.call(:kind=>1).should be_a_instance_of(StiTest3)
      StiTest2.dataset.row_proc.call(:kind=>2).should be_a_instance_of(StiTest4)

      StiTest2.create.kind.should == 2
      StiTest3.create.kind.should == 1
      StiTest4.create.kind.should == 2
    end

    it "should work with custom procs with symbols" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>proc{|v| v == 1 ? :StiTest3 : :StiTest4}, :key_map=>proc{|klass| klass.name == 'StiTest3' ? 1 : 2}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
      StiTest2.dataset.row_proc.call(:kind=>0).should be_a_instance_of(StiTest4)
      StiTest2.dataset.row_proc.call(:kind=>1).should be_a_instance_of(StiTest3)
      StiTest2.dataset.row_proc.call(:kind=>2).should be_a_instance_of(StiTest4)

      StiTest2.create.kind.should == 2
      StiTest3.create.kind.should == 1
      StiTest4.create.kind.should == 2
    end

    it "should work with custom hashes" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>{0=>StiTest2, 1=>:StiTest3, 2=>'StiTest4'}, :key_map=>{StiTest2=>4, 'StiTest3'=>5, 'StiTest4'=>6}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
      StiTest2.dataset.row_proc.call(:kind=>0).should be_a_instance_of(StiTest2)
      StiTest2.dataset.row_proc.call(:kind=>1).should be_a_instance_of(StiTest3)
      StiTest2.dataset.row_proc.call(:kind=>2).should be_a_instance_of(StiTest4)

      StiTest2.create.kind.should == 4
      StiTest3.create.kind.should == 5
      StiTest4.create.kind.should == 6
    end

    it "should infer key_map from model_map if provided as a hash" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>{0=>StiTest2, 1=>'StiTest3', 2=>:StiTest4}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
      StiTest2.dataset.row_proc.call(:kind=>0).should be_a_instance_of(StiTest2)
      StiTest2.dataset.row_proc.call(:kind=>1).should be_a_instance_of(StiTest3)
      StiTest2.dataset.row_proc.call(:kind=>2).should be_a_instance_of(StiTest4)

      StiTest2.create.kind.should == 0
      StiTest3.create.kind.should == 1
      StiTest4.create.kind.should == 2
    end

    it "should raise exceptions if a bad model value is used" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>{0=>1,1=>1.5, 2=>Date.today}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
      proc{StiTest2.dataset.row_proc.call(:kind=>0)}.should raise_error(Sequel::Error)
      proc{StiTest2.dataset.row_proc.call(:kind=>1)}.should raise_error(Sequel::Error)
      proc{StiTest2.dataset.row_proc.call(:kind=>2)}.should raise_error(Sequel::Error)
    end
  end
end
