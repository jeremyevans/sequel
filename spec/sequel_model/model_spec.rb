require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model do
  it "should have class method aliased as model" do
    Sequel::Model.instance_methods.collect{|x| x.to_s}.should include("model")

    model_a = Class.new(Sequel::Model(:items))
    model_a.new.model.should be(model_a)
  end

  it "should be associated with a dataset" do
    model_a = Class.new(Sequel::Model) { set_dataset MODEL_DB[:as] }

    model_a.dataset.should be_a_kind_of(MockDataset)
    model_a.dataset.opts[:from].should == [:as]

    model_b = Class.new(Sequel::Model) { set_dataset MODEL_DB[:bs] }

    model_b.dataset.should be_a_kind_of(MockDataset)
    model_b.dataset.opts[:from].should == [:bs]

    model_a.dataset.opts[:from].should == [:as]
  end

end

describe Sequel::Model, "dataset & schema" do
  before do
    @model = Class.new(Sequel::Model(:items))
  end

  it "creates dynamic model subclass with set table name" do
    @model.table_name.should == :items
  end

  it "defaults to primary key of id" do
    @model.primary_key.should == :id
  end

  it "allow primary key change" do
    @model.set_primary_key :ssn
    @model.primary_key.should == :ssn
  end

  it "allows dataset change" do
    @model.set_dataset(MODEL_DB[:foo])
    @model.table_name.should == :foo
  end

  it "set_dataset should take a symbol" do
    @model.db = MODEL_DB
    @model.set_dataset(:foo)
    @model.table_name.should == :foo
  end

  it "set_dataset should raise an error unless given a Symbol or Dataset" do
    proc{@model.set_dataset(Object.new)}.should raise_error(Sequel::Error)
  end

  it "set_dataset should add the destroy method to the dataset" do
    ds = MODEL_DB[:foo]
    ds.should_not respond_to(:destroy)
    @model.set_dataset(ds)
    ds.should respond_to(:destroy)
  end

  it "sets schema with implicit table name" do
    @model.set_schema do
      primary_key :ssn, :string
    end
    @model.primary_key.should == :ssn
    @model.table_name.should == :items
  end

  it "sets schema with explicit table name" do
    @model.set_schema :foo do
      primary_key :id
    end
    @model.primary_key.should == :id
    @model.table_name.should == :foo
  end

  it "doesn't raise an error on set_dataset if there is an error raised getting the schema" do
    @model.meta_def(:get_db_schema){raise Sequel::Error}
    proc{@model.set_dataset(MODEL_DB[:foo])}.should_not raise_error
  end

  it "doesn't raise an error on inherited if there is an error setting the dataset" do
    @model.meta_def(:set_dataset){raise Sequel::Error}
    proc{Class.new(@model)}.should_not raise_error
  end
end

describe Sequel::Model, "#sti_key" do
  before do
    class ::StiTest < Sequel::Model
      def kind=(x); self[:kind] = x; end
      def refresh; end
      set_sti_key :kind
    end
    class ::StiTestSub1 < StiTest
    end
    class ::StiTestSub2 < StiTest
    end
    @ds = StiTest.dataset
    MODEL_DB.reset
  end
  
  it "should return rows with the correct class based on the polymorphic_key value" do
    def @ds.fetch_rows(sql)
      yield({:kind=>'StiTest'})
      yield({:kind=>'StiTestSub1'})
      yield({:kind=>'StiTestSub2'})
    end
    StiTest.all.collect{|x| x.class}.should == [StiTest, StiTestSub1, StiTestSub2]
  end

  it "should fallback to the main class if polymophic_key value is NULL" do
    def @ds.fetch_rows(sql)
      yield({:kind=>nil})
    end
    StiTest.all.collect{|x| x.class}.should == [StiTest]
  end
  
  it "should fallback to the main class if the given class does not exist" do
    def @ds.fetch_rows(sql)
      yield({:kind=>'StiTestSub3'})
    end
    StiTest.all.collect{|x| x.class}.should == [StiTest]
  end
  
  it "should add a before_create hook that sets the model class name for the key" do
    StiTest.new.save
    StiTestSub1.new.save
    StiTestSub2.new.save
    MODEL_DB.sqls.should == ["INSERT INTO sti_tests (kind) VALUES ('StiTest')", "INSERT INTO sti_tests (kind) VALUES ('StiTestSub1')", "INSERT INTO sti_tests (kind) VALUES ('StiTestSub2')"]
  end
  
  it "should add a filter to model datasets inside subclasses hook to only retreive objects with the matching key" do
    StiTest.dataset.sql.should == "SELECT * FROM sti_tests"
    StiTestSub1.dataset.sql.should == "SELECT * FROM sti_tests WHERE (kind = 'StiTestSub1')"
    StiTestSub2.dataset.sql.should == "SELECT * FROM sti_tests WHERE (kind = 'StiTestSub2')"
  end
end

describe Sequel::Model, "constructor" do
  
  before(:each) do
    @m = Class.new(Sequel::Model)
    @m.columns :a, :b
  end

  it "should accept a hash" do
    m = @m.new(:a => 1, :b => 2)
    m.values.should == {:a => 1, :b => 2}
    m.should be_new
  end
  
  it "should accept a block and yield itself to the block" do
    block_called = false
    m = @m.new {|i| block_called = true; i.should be_a_kind_of(@m); i.values[:a] = 1}
    
    block_called.should be_true
    m.values[:a].should == 1
  end
  
end

describe Sequel::Model, "new" do

  before(:each) do
    @m = Class.new(Sequel::Model) do
      set_dataset MODEL_DB[:items]
      columns :x, :id
    end
  end

  it "should be marked as new?" do
    o = @m.new
    o.should be_new
  end

  it "should not be marked as new? once it is saved" do
    o = @m.new(:x => 1)
    o.should be_new
    o.save
    o.should_not be_new
  end

  it "should use the last inserted id as primary key if not in values" do
    d = @m.dataset
    def d.insert(*args)
      super
      1234
    end

    def d.first
      {:x => 1, :id => 1234}
    end

    o = @m.new(:x => 1)
    o.save
    o.id.should == 1234

    o = @m.load(:x => 1, :id => 333)
    o.save
    o.id.should == 333
  end

end

describe Sequel::Model, ".subset" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
  end

  specify "should create a filter on the underlying dataset" do
    proc {@c.new_only}.should raise_error(NoMethodError)
    
    @c.subset(:new_only) {:age.sql_number < 'new'}
    
    @c.new_only.sql.should == "SELECT * FROM items WHERE (age < 'new')"
    @c.dataset.new_only.sql.should == "SELECT * FROM items WHERE (age < 'new')"
    
    @c.subset(:pricey) {:price.sql_number > 100}
    
    @c.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    @c.dataset.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    
    @c.pricey.new_only.sql.should == "SELECT * FROM items WHERE ((price > 100) AND (age < 'new'))"
    @c.new_only.pricey.sql.should == "SELECT * FROM items WHERE ((age < 'new') AND (price > 100))"
  end

end

describe Sequel::Model, ".find" do

  before(:each) do
    MODEL_DB.reset
    
    @c = Class.new(Sequel::Model(:items))
    
    $cache_dataset_row = {:name => 'sharon', :id => 1}
    @dataset = @c.dataset
    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield $cache_dataset_row
      end
    })
  end
  
  it "should return the first record matching the given filter" do
    @c.find(:name => 'sharon').should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"

    @c.find(:name.like('abc%')).should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (name LIKE 'abc%') LIMIT 1"
  end
  
  specify "should accept filter blocks" do
    @c.find{:id.sql_number > 1}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id > 1) LIMIT 1"

    @c.find {(:x.sql_number > 1) & (:y.sql_number < 2)}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE ((x > 1) AND (y < 2)) LIMIT 1"
  end

end

describe Sequel::Model, ".fetch" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items))
  end
  
  it "should return instances of Model" do
    @c.fetch("SELECT * FROM items").first.should be_a_kind_of(@c)
  end

  it "should return true for .empty? and not raise an error on empty selection" do
    rows = @c.fetch("SELECT * FROM items WHERE FALSE")
    @c.class_def(:fetch_rows) {|sql| yield({:count => 0})}
    proc {rows.empty?}.should_not raise_error
  end
  
end

describe Sequel::Model, ".find_or_create" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      columns :x
    end
  end

  it "should find the record" do
    @c.find_or_create(:x => 1)
    MODEL_DB.sqls.should == ["SELECT * FROM items WHERE (x = 1) LIMIT 1"]
    
    MODEL_DB.reset
  end
  
  it "should create the record if not found" do
    @c.meta_def(:find) do |*args|
      dataset.filter(*args).first
      nil
    end
    
    @c.find_or_create(:x => 1)
    MODEL_DB.sqls.should == [
      "SELECT * FROM items WHERE (x = 1) LIMIT 1",
      "INSERT INTO items (x) VALUES (1)"
    ]
  end
end

describe Sequel::Model, ".delete_all" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end
    
    @c.dataset.meta_def(:delete) {MODEL_DB << delete_sql}
  end

  deprec_specify "should delete all records in the dataset" do
    @c.delete_all
    MODEL_DB.sqls.should == ["DELETE FROM items"]
  end

end

describe Sequel::Model, ".destroy_all" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end

    @c.dataset.meta_def(:delete) {MODEL_DB << delete_sql}
  end

  deprec_specify "should delete all records in the dataset" do
    @c.dataset.meta_def(:destroy) {MODEL_DB << "DESTROY this stuff"}
    @c.destroy_all
    MODEL_DB.sqls.should == ["DESTROY this stuff"]
  end
  
  deprec_specify "should call dataset.destroy" do
    @c.dataset.should_receive(:destroy).and_return(true)
    @c.destroy_all
  end
end

describe Sequel::Model, ".all" do
  
  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end
    
    @c.dataset.meta_def(:all) {1234}
  end
  
  it "should return all records in the dataset" do
    @c.all.should == 1234
  end
  
end

class DummyModelBased < Sequel::Model(:blog)
end

describe Sequel::Model, "(:tablename)" do

  it "should allow reopening of descendant classes" do
    proc do
      eval "class DummyModelBased < Sequel::Model(:blog); end"
    end.should_not raise_error
  end

end

describe Sequel::Model, "A model class without a primary key" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      columns :x
      no_primary_key
    end
  end

  it "should be able to insert records without selecting them back" do
    i = nil
    proc {i = @c.create(:x => 1)}.should_not raise_error
    i.class.should be(@c)
    i.values.to_hash.should == {:x => 1}

    MODEL_DB.sqls.should == ['INSERT INTO items (x) VALUES (1)']
  end

  it "should raise when deleting" do
    o = @c.new
    proc {o.delete}.should raise_error
  end

  it "should insert a record when saving" do
    o = @c.new(:x => 2)
    o.should be_new
    o.save
    MODEL_DB.sqls.should == ['INSERT INTO items (x) VALUES (2)']
  end

end

describe Sequel::Model, "attribute accessors" do
  before do
    MODEL_DB.reset
    @dataset = Sequel::Dataset.new(MODEL_DB)
    def @dataset.columns; [:x, :y]; end
    @c = Class.new(Sequel::Model) do
      def self.db_schema
         set_columns(Array(@columns))
        @db_schema = {:x=>{}, :y=>{}}
      end
      def self.set_dataset(ds, opts={}) 
        @columns = ds.columns
        db_schema
      end
    end
  end

  it "should be created on set_dataset" do
    %w'x y x= y='.each do |x|
      @c.instance_methods.collect{|y| y.to_s}.should_not include(x)
    end
    @c.set_dataset(@dataset)
    %w'x y x= y='.each do |x|
      @c.instance_methods.collect{|y| y.to_s}.should include(x)
    end
    o = @c.new
    %w'x y x= y='.each do |x|
      o.methods.collect{|y| y.to_s}.should include(x)
    end

    o.x.should be_nil
    o.x = 34
    o.x.should == 34
  end

  it "should be only accept one argument for the write accessor" do
    @c.set_dataset(@dataset)
    o = @c.new

    o.x = 34
    o.x.should == 34
    proc{o.send(:x=)}.should raise_error
    proc{o.send(:x=, 3, 4)}.should raise_error
  end
end

describe Sequel::Model, ".[]" do

  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))

    $cache_dataset_row = {:name => 'sharon', :id => 1}
    @dataset = @c.dataset
    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield $cache_dataset_row
      end
    })
  end

  it "should return the first record for the given pk" do
    @c[1].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 1) LIMIT 1"
    @c[9999].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 9999) LIMIT 1"
  end

  it "should work correctly for custom primary key" do
    @c.set_primary_key :name
    @c['sharon'].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"
  end

  it "should work correctly for composite primary key" do
    @c.set_primary_key [:node_id, :kind]
    @c[3921, 201].should be_a_kind_of(@c)
    $sqls.last.should =~ \
    /^SELECT \* FROM items WHERE \((\(node_id = 3921\) AND \(kind = 201\))|(\(kind = 201\) AND \(node_id = 3921\))\) LIMIT 1$/
  end
end

context "Model#inspect" do
  setup do
    @o = Sequel::Model.load(:x => 333)
  end
  
  specify "should include the class name and the values" do
    @o.inspect.should == '#<Sequel::Model @values={:x=>333}>'
  end
end

context "Model.db_schema" do
  setup do
    @c = Class.new(Sequel::Model(:items)) do
      def self.columns; orig_columns; end
    end
    @dataset = Sequel::Dataset.new(nil).from(:items)
    @dataset.meta_def(:db){@db ||= Sequel::Database.new}
    def @dataset.naked; self; end
    def @dataset.columns; []; end
    def @dataset.def_mutation_method(*names);  end
  end
  
  specify "should use the database's schema_for_table and set the columns and dataset columns" do
    d = @dataset.db
    def d.schema(table, opts = {})
      [[:x, {:type=>:integer}], [:y, {:type=>:string}]]
    end
    @c.dataset = @dataset
    @c.db_schema.should == {:x=>{:type=>:integer}, :y=>{:type=>:string}}
    @c.columns.should == [:x, :y]
    @c.dataset.instance_variable_get(:@columns).should == [:x, :y]
  end

  specify "should restrict the schema and columns for datasets with a :select option" do
    ds = @dataset.select(:x, :y___z)
    d = ds.db
    def d.schema(table, opts = {})
      [[:x, {:type=>:integer}], [:y, {:type=>:string}]]
    end
    def @c.columns; [:x, :z]; end
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:type=>:integer}, :z=>{}}
  end

  specify "should not use schema_for_table if the dataset uses multiple tables or custom sql" do
    ds = @dataset.join(:x, :id)
    d = ds.db
    e = false
    d.meta_def(:schema){|table| e = true}
    def @c.columns; [:x]; end
    @c.dataset = ds
    @c.db_schema.should == {:x=>{}}
    e.should == false
  end

  specify "should fallback to fetching records if schema_for_table raises an error" do
    ds = @dataset.join(:x, :id)
    d = ds.db
    def d.schema(table)
      raise StandardError
    end
    def @c.columns; [:x]; end
    @c.dataset = ds
    @c.db_schema.should == {:x=>{}}
  end
end

context "Model.str_columns" do
  deprec_specify "should return the columns as frozen strings" do
    c = Class.new(Sequel::Model)
    c.meta_def(:columns){[:a, :b]}
    c.orig_str_columns.should == %w'a b'
    proc{c.orig_str_columns.first << 'a'}.should raise_error
  end
end
