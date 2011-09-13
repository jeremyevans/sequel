require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Model()" do
  before do
    @db = Sequel::Model.db
  end

  it "should return a model subclass with the given dataset if given a dataset" do
    ds = @db[:blah]
    c = Sequel::Model(ds)
    c.superclass.should == Sequel::Model
    c.dataset.should == ds
  end

  it "should return a model subclass with a dataset with the default database and given table name if given a Symbol" do
    c = Sequel::Model(:blah)
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == :blah
  end

  it "should return a model subclass with a dataset with the default database and given table name if given a LiteralString" do
    c = Sequel::Model('blah'.lit)
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == 'blah'.lit
  end

  it "should return a model subclass with a dataset with the default database and given table name if given an SQL::Identifier" do
    c = Sequel::Model(:blah.identifier)
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == :blah.identifier
  end

  it "should return a model subclass with a dataset with the default database and given table name if given an SQL::QualifiedIdentifier" do
    c = Sequel::Model(:blah.qualify(:boo))
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == :blah.qualify(:boo)
  end

  it "should return a model subclass with a dataset with the default database and given table name if given an SQL::AliasedExpression" do
    c = Sequel::Model(:blah.as(:boo))
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == :boo
  end

  it "should return a model subclass with the given dataset if given a dataset using an SQL::Identifier" do
    ds = @db[:blah.identifier]
    c = Sequel::Model(ds)
    c.superclass.should == Sequel::Model
    c.dataset.should == ds
  end

  it "should return a model subclass associated to the given database if given a database" do
    db = Sequel::Database.new
    c = Sequel::Model(db)
    c.superclass.should == Sequel::Model
    c.db.should == db
    proc{c.dataset}.should raise_error(Sequel::Error)
    class SmBlahTest < c
    end
    SmBlahTest.db.should == db
    SmBlahTest.table_name.should == :sm_blah_tests
  end

  describe "reloading" do
    after do
      Object.send(:remove_const, :Album) if defined?(::Album)
    end

    it "should work without raising an exception with a symbol" do
      proc do
        class ::Album < Sequel::Model(:table); end
        class ::Album < Sequel::Model(:table); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an SQL::Identifier " do
      proc do
        class ::Album < Sequel::Model(:table.identifier); end
        class ::Album < Sequel::Model(:table.identifier); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an SQL::QualifiedIdentifier " do
      proc do
        class ::Album < Sequel::Model(:table.qualify(:schema)); end
        class ::Album < Sequel::Model(:table.qualify(:schema)); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an SQL::AliasedExpression" do
      proc do
        class ::Album < Sequel::Model(:table.as(:alias)); end
        class ::Album < Sequel::Model(:table.as(:alias)); end
      end.should_not raise_error
    end

    it "should work without raising an exception with a database" do
      proc do
        class ::Album < Sequel::Model(@db); end
        class ::Album < Sequel::Model(@db); end
      end.should_not raise_error
    end

    it "should work without raising an exception with a dataset" do
      proc do
        class ::Album < Sequel::Model(@db[:table]); end
        class ::Album < Sequel::Model(@db[:table]); end
      end.should_not raise_error
    end

    it "should work without raising an exception with a dataset with an SQL::Identifier" do
      proc do
        class ::Album < Sequel::Model(@db[:table.identifier]); end
        class ::Album < Sequel::Model(@db[:table.identifier]); end
      end.should_not raise_error
    end
  end
end

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

  it "set_dataset should take a Symbol" do
    @model.db = MODEL_DB
    @model.set_dataset(:foo)
    @model.table_name.should == :foo
  end

  it "set_dataset should take a LiteralString" do
    @model.db = MODEL_DB
    @model.set_dataset('foo'.lit)
    @model.table_name.should == 'foo'.lit
  end

  it "set_dataset should take an SQL::Identifier" do
    @model.db = MODEL_DB
    @model.set_dataset(:foo.identifier)
    @model.table_name.should == :foo.identifier
  end

  it "set_dataset should take an SQL::QualifiedIdentifier" do
    @model.db = MODEL_DB
    @model.set_dataset(:foo.qualify(:bar))
    @model.table_name.should == :foo.qualify(:bar)
  end

  it "set_dataset should take an SQL::AliasedExpression" do
    @model.db = MODEL_DB
    @model.set_dataset(:foo.as(:bar))
    @model.table_name.should == :bar
  end

  it "table_name should respect table aliases" do
    @model.set_dataset(:foo___x)
    @model.table_name.should == :x
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

  it "should raise an error on set_dataset if there is an error connecting to the database" do
    @model.meta_def(:columns){raise Sequel::DatabaseConnectionError}
    proc{@model.set_dataset(MODEL_DB[:foo].join(:blah))}.should raise_error
  end

  it "should not raise an error if there is a problem getting the columns for a dataset" do
    @model.meta_def(:columns){raise Sequel::Error}
    proc{@model.set_dataset(MODEL_DB[:foo].join(:blah))}.should_not raise_error
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

  specify "should not override existing model methods" do
    @c.meta_def(:active){true}
    @c.subset(:active, :active)
    @c.active.should == true
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
    @c.send(:define_method, :fetch_rows){|sql| yield({:count => 0})}
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

  it "should pass the new record to be created to the block if no record is found" do
    @c.meta_def(:find){|*|} 
    @c.find_or_create(:x => 1){|x| x[:y] = 2}
    ["INSERT INTO items (x, y) VALUES (1, 2)", "INSERT INTO items (y, x) VALUES (2, 1)"].should include(MODEL_DB.sqls.first)
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
        @db_schema = {:x=>{:type=>:integer}, :y=>{:type=>:integer}}
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

  it "should have a working typecasting setter even if the column is not selected" do
    @c.set_dataset(@dataset.select(:y))
    o = @c.new

    o.x = '34'
    o.x.should == 34
  end

  it "should typecast if the new value is the same as the existing but has a different class" do
    @c.set_dataset(@dataset.select(:y))
    o = @c.new

    o.x = 34
    o.x = 34.0
    o.x.should == 34.0
    o.x = 34
    o.x.should == 34
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

  it "should work correctly for composite primary key specified as array" do
    @c.set_primary_key [:node_id, :kind]
    @c[3921, 201].should be_a_kind_of(@c)
    $sqls.last.should =~ \
    /^SELECT \* FROM items WHERE \((\(node_id = 3921\) AND \(kind = 201\))|(\(kind = 201\) AND \(node_id = 3921\))\) LIMIT 1$/
  end
  
  it "should work correctly for composite primary key specified as separate arguments" do
    @c.set_primary_key :node_id, :kind
    @c[3921, 201].should be_a_kind_of(@c)
    $sqls.last.should =~ \
    /^SELECT \* FROM items WHERE \((\(node_id = 3921\) AND \(kind = 201\))|(\(kind = 201\) AND \(node_id = 3921\))\) LIMIT 1$/
  end
end

describe "Model#inspect" do
  before do
    @o = Sequel::Model.load(:x => 333)
  end
  
  specify "should include the class name and the values" do
    @o.inspect.should == '#<Sequel::Model @values={:x=>333}>'
  end
end

describe "Model.db_schema" do
  before do
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

  specify "should not restrict the schema for datasets with a :select option" do
    ds = @dataset.select(:x, :y___z)
    d = ds.db
    def d.schema(table, opts = {})
      [[:x, {:type=>:integer}], [:y, {:type=>:string}]]
    end
    def @c.columns; [:x, :z]; end
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:type=>:integer}, :z=>{}, :y=>{:type=>:string}}
  end

  specify "should not use schema if the dataset uses multiple tables or custom sql" do
    ds = @dataset.join(:x, :id)
    d = ds.db
    e = false
    d.meta_def(:schema){|table, *opts| e = true}
    def @c.columns; [:x]; end
    @c.dataset = ds
    @c.db_schema.should == {:x=>{}}
    e.should == false
  end

  specify "should fallback to fetching records if schema raises an error" do
    ds = @dataset.join(:x, :id)
    d = ds.db
    def d.schema(table, opts={})
      raise StandardError
    end
    def @c.columns; [:x]; end
    @c.dataset = ds
    @c.db_schema.should == {:x=>{}}
  end
  
  specify "should automatically set a singular primary key based on the schema" do
    ds = @dataset
    d = ds.db
    d.meta_def(:schema){|table, *opts| [[:x, {:primary_key=>true}]]}
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>true}}
    @c.primary_key.should == :x
  end
  
  specify "should automatically set the composite primary key based on the schema" do
    ds = @dataset
    d = ds.db
    d.meta_def(:schema){|table, *opts| [[:x, {:primary_key=>true}], [:y, {:primary_key=>true}]]}
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>true}, :y=>{:primary_key=>true}}
    @c.primary_key.should == [:x, :y]
  end
  
  specify "should automatically set no primary key based on the schema" do
    ds = @dataset
    d = ds.db
    d.meta_def(:schema){|table, *opts| [[:x, {:primary_key=>false}], [:y, {:primary_key=>false}]]}
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>false}, :y=>{:primary_key=>false}}
    @c.primary_key.should == nil
  end
  
  specify "should not modify the primary key unless all column schema hashes have a :primary_key entry" do
    ds = @dataset
    d = ds.db
    d.meta_def(:schema){|table, *opts| [[:x, {:primary_key=>false}], [:y, {}]]}
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>false}, :y=>{}}
    @c.primary_key.should == :id
  end
end

describe "Model#use_transactions" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end

  specify "should return class value by default" do
    @c.use_transactions = true
    @c.new.use_transactions.should == true
    @c.use_transactions = false
    @c.new.use_transactions.should == false
  end

  specify "should return set value if manually set" do
    instance = @c.new
    instance.use_transactions = false
    instance.use_transactions.should == false
    @c.use_transactions = true
    instance.use_transactions.should == false
    
    instance.use_transactions = true
    instance.use_transactions.should == true
    @c.use_transactions = false
    instance.use_transactions.should == true
  end
end
