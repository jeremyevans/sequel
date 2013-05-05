require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "single table inheritance plugin" do
  before do
    class ::StiTest < Sequel::Model
      columns :id, :kind, :blah
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
    StiTest.simple_table.should == "sti_tests"
    StiTestSub1.simple_table.should == nil
  end
  
  it "should allow changing the inheritance column via a plugin :single_table_inheritance call" do
    StiTest.plugin :single_table_inheritance, :blah
    Object.send(:remove_const, :StiTestSub1)
    Object.send(:remove_const, :StiTestSub2)
    class ::StiTestSub1 < StiTest; end 
    class ::StiTestSub2 < StiTest; end 
    @ds._fetch = [{:blah=>'StiTest'}, {:blah=>'StiTestSub1'}, {:blah=>'StiTestSub2'}]
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTestSub1, StiTestSub2]
    StiTest.dataset.sql.should == "SELECT * FROM sti_tests"
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.blah IN ('StiTestSub1'))"
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (sti_tests.blah IN ('StiTestSub2'))"
  end 
  
  it "should return rows with the correct class based on the polymorphic_key value" do
    @ds._fetch = [{:kind=>'StiTest'}, {:kind=>'StiTestSub1'}, {:kind=>'StiTestSub2'}]
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTestSub1, StiTestSub2]
  end 

  it "should return rows with the correct class based on the polymorphic_key value when retreiving by primary key" do
    @ds._fetch = [{:kind=>'StiTestSub1'}]
    StiTest[1].class.should == StiTestSub1
  end 

  it "should return rows with the correct class for subclasses based on the polymorphic_key value" do
    class ::StiTestSub1Sub < StiTestSub1; end 
    StiTestSub1.dataset._fetch = [{:kind=>'StiTestSub1'}, {:kind=>'StiTestSub1Sub'}]
    StiTestSub1.all.collect{|x| x.class}.should == [StiTestSub1, StiTestSub1Sub]
  end 

  it "should fallback to the main class if the given class does not exist" do
    @ds._fetch = {:kind=>'StiTestSub3'}
    StiTest.all.collect{|x| x.class}.should == [StiTest]
  end

  it "should fallback to the main class if the sti_key field is empty or nil without calling constantize" do
    called = false
    StiTest.meta_def(:constantize) do |s|
      called = true
      Object
    end
    StiTest.plugin :single_table_inheritance, :kind
    @ds._fetch = [{:kind=>''}, {:kind=>nil}]
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTest]
    called.should == false
  end

  it "should add a before_create hook that sets the model class name for the key" do
    StiTest.new.save
    StiTestSub1.new.save
    StiTestSub2.new.save
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (kind) VALUES ('StiTest')", "SELECT * FROM sti_tests WHERE (id = 10) LIMIT 1", "INSERT INTO sti_tests (kind) VALUES ('StiTestSub1')", "SELECT * FROM sti_tests WHERE ((sti_tests.kind IN ('StiTestSub1')) AND (id = 10)) LIMIT 1", "INSERT INTO sti_tests (kind) VALUES ('StiTestSub2')", "SELECT * FROM sti_tests WHERE ((sti_tests.kind IN ('StiTestSub2')) AND (id = 10)) LIMIT 1"]
  end

  it "should have the before_create hook not override an existing value" do
    StiTest.create(:kind=>'StiTestSub1')
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (kind) VALUES ('StiTestSub1')", "SELECT * FROM sti_tests WHERE (id = 10) LIMIT 1"]
  end

  it "should have the before_create hook handle columns with the same name as existing method names" do
    StiTest.plugin :single_table_inheritance, :type
    StiTest.columns :id, :type
    StiTest.create
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (type) VALUES ('StiTest')", "SELECT * FROM sti_tests WHERE (id = 10) LIMIT 1"]
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
        def _save_refresh; end
      end
    end
    after do
      Object.send(:remove_const, :StiTest2)
      Object.send(:remove_const, :StiTest3)
      Object.send(:remove_const, :StiTest4)
    end

    specify "should have working row_proc if using set_dataset in subclass to remove columns" do
      StiTest2.plugin :single_table_inheritance, :kind
      class ::StiTest3 < ::StiTest2
        set_dataset(dataset.select(*(columns - [:blah])))
      end
      class ::StiTest4 < ::StiTest3; end
      StiTest3.dataset._fetch = {:id=>1, :kind=>'StiTest4'}
      StiTest3[1].should == StiTest4.load(:id=>1, :kind=>'StiTest4')
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

      class ::StiTest5 < ::StiTest4; end
      StiTest5.create.kind.should == nil
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

    it "should work with non-bijective mappings" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>{0=>'StiTest3', 1=>'StiTest3', 2=>'StiTest4'}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
      StiTest2.dataset.row_proc.call(:kind=>0).should be_a_instance_of(StiTest3)
      StiTest2.dataset.row_proc.call(:kind=>1).should be_a_instance_of(StiTest3)
      StiTest2.dataset.row_proc.call(:kind=>2).should be_a_instance_of(StiTest4)

      [0,1].should include(StiTest3.create.kind)
      StiTest4.create.kind.should == 2
    end

    it "should work with non-bijective mappings and key map procs" do
      StiTest2.plugin :single_table_inheritance, :kind,
        :key_map=>proc{|model| model.to_s == 'StiTest4' ? 2 : [0,1] }
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end

      StiTest2.dataset.sql.should == "SELECT * FROM sti_test2s"
      StiTest3.dataset.sql.should == "SELECT * FROM sti_test2s WHERE (sti_test2s.kind IN (0, 1))"
      StiTest4.dataset.sql.should == "SELECT * FROM sti_test2s WHERE (sti_test2s.kind IN (2))"
    end

    it "should create correct sql with non-bijective mappings" do
      StiTest2.plugin :single_table_inheritance, :kind, :model_map=>{0=>'StiTest3', 1=>'StiTest3', 2=>'StiTest4'}
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end
    
      StiTest2.dataset.sql.should == "SELECT * FROM sti_test2s"
      ["SELECT * FROM sti_test2s WHERE (sti_test2s.kind IN (0, 1))",
       "SELECT * FROM sti_test2s WHERE (sti_test2s.kind IN (1, 0))"].should include(StiTest3.dataset.sql)
    end

    it "should honor a :key_chooser" do
      StiTest2.plugin :single_table_inheritance, :kind, :key_chooser => proc{|inst| inst.model.to_s.downcase }
      class ::StiTest3 < ::StiTest2; end
      class ::StiTest4 < ::StiTest2; end

      StiTest3.create.kind.should == 'stitest3'
      StiTest4.create.kind.should == 'stitest4'
    end

  end
end
