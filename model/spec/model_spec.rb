require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model do
  it "should have class method aliased as model" do
    Sequel::Model.instance_methods.should include("model")

    model_a = Class.new Sequel::Model
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

  it "puts the lotion in the basket or it gets the hose again" do
    # just kidding!
  end
end

describe Sequel::Model, "constructor" do
  
  before(:each) do
    @m = Class.new(Sequel::Model)
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
    end
  end

  it "should be marked as new?" do
    o = @m.new
    o.should be_new
    o.should be_new_record
  end

  it "should not be marked as new? once it is saved" do
    o = @m.new(:x => 1)
    o.should be_new
    o.save
    o.should_not be_new
    o.should_not be_new_record
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

    o = @m.new(:x => 1, :id => 333)
    o.save
    o.id.should == 333
  end

end

describe Sequel::Model, ".subset" do

  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
  end

  it "should create a filter on the underlying dataset" do
    proc {@c.new_only}.should raise_error(NoMethodError)
    
    @c.subset(:new_only) {:age == 'new'}
    
    @c.new_only.sql.should == "SELECT * FROM items WHERE (age = 'new')"
    @c.dataset.new_only.sql.should == "SELECT * FROM items WHERE (age = 'new')"
    
    @c.subset(:pricey) {:price > 100}
    
    @c.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    @c.dataset.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    
    # check if subsets are composable
    @c.pricey.new_only.sql.should == "SELECT * FROM items WHERE (price > 100) AND (age = 'new')"
    @c.new_only.pricey.sql.should == "SELECT * FROM items WHERE (age = 'new') AND (price > 100)"
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

    @c.find {"name LIKE 'abc%'".lit}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE name LIKE 'abc%' LIMIT 1"
  end
  
  it "should accept filter blocks" do
    @c.find {:id == 1}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 1) LIMIT 1"

    @c.find {:x > 1 && :y < 2}.should be_a_kind_of(@c)
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

describe Sequel::Model, "magic methods" do

  before(:each) do
    @c = Class.new(Sequel::Dataset) do
      @@sqls = []
    
      def self.sqls; @@sqls; end
    
      def fetch_rows(sql)
        @@sqls << sql
        yield({:id => 123, :name => 'hey'})
      end
    end
  
    @m = Class.new(Sequel::Model(@c.new(nil).from(:items)))
  end
  
  it "should support order_by_xxx" do
    @m.order_by_name.should be_a_kind_of(@c)
    @m.order_by_name.sql.should == "SELECT * FROM items ORDER BY name"
  end

  it "should support group_by_xxx" do
    @m.group_by_name.should be_a_kind_of(@c)
    @m.group_by_name.sql.should == "SELECT * FROM items GROUP BY name"
  end

  it "should support count_by_xxx" do
    @m.count_by_name.should be_a_kind_of(@c)
    @m.count_by_name.sql.should == "SELECT name, count(name) AS count FROM items GROUP BY name ORDER BY count"
  end

  it "should support filter_by_xxx" do
    @m.filter_by_name('sharon').should be_a_kind_of(@c)
    @m.filter_by_name('sharon').sql.should == "SELECT * FROM items WHERE (name = 'sharon')"
  end
  
  it "should support all_by_xxx" do
    all = @m.all_by_name('sharon')
    all.class.should == Array
    all.size.should == 1
    all.first.should be_a_kind_of(@m)
    all.first.values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items WHERE (name = 'sharon')"]
  end
  
  it "should support find_by_xxx" do
    @m.find_by_name('sharon').should be_a_kind_of(@m)
    @m.find_by_name('sharon').values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"] * 2
  end

  it "should support first_by_xxx" do
    @m.first_by_name('sharon').should be_a_kind_of(@m)
    @m.first_by_name('sharon').values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items ORDER BY name LIMIT 1"] * 2
  end

  it "should support last_by_xxx" do
    @m.last_by_name('sharon').should be_a_kind_of(@m)
    @m.last_by_name('sharon').values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items ORDER BY name DESC LIMIT 1"] * 2
  end
  
end

describe Sequel::Model, ".find_or_create" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
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

  it "should delete all records in the dataset" do
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

  it "should delete all records in the dataset" do
    @c.destroy_all
    MODEL_DB.sqls.should == ["DELETE FROM items"]
  end
  
  it "should call dataset destroy method if *_destroy hooks exist" do
    @c.dataset.stub!(:destroy).and_return(true)
    @c.should_receive(:has_hooks?).with(:before_destroy).and_return(true)
    @c.destroy_all
  end
  
  it "should call dataset delete method if no hooks are present" do
    @c.dataset.stub!(:delete).and_return(true)
    @c.should_receive(:has_hooks?).with(:before_destroy).and_return(false)
    @c.should_receive(:has_hooks?).with(:after_destroy).and_return(false)
    @c.destroy_all
  end

end

describe Sequel::Model, ".join" do

  before(:each) do
    MODEL_DB.reset  
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end
  end
  
  it "should format proper SQL" do
    @c.join(:atts, :item_id => :id).sql.should == \
      "SELECT items.* FROM items INNER JOIN atts ON (atts.item_id = items.id)"
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

describe Sequel::Model, ".create" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items))
  end

  it "should be able to create rows in the associated table" do
    o = @c.create(:x => 1)
    o.class.should == @c
    MODEL_DB.sqls.should == ['INSERT INTO items (x) VALUES (1)',  "SELECT * FROM items WHERE (id IN ('INSERT INTO items (x) VALUES (1)')) LIMIT 1"]
  end

  it "should be able to create rows without any values specified" do
    o = @c.create
    o.class.should == @c
    MODEL_DB.sqls.should == ["INSERT INTO items DEFAULT VALUES", "SELECT * FROM items WHERE (id IN ('INSERT INTO items DEFAULT VALUES')) LIMIT 1"]
  end

end

describe Sequel::Model, "A model class without a primary key" do

  before(:each) do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
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

  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
  end

  it "should be created dynamically" do
    o = @c.new

    o.should_not be_respond_to(:x)
    o.x.should be_nil
    o.should be_respond_to(:x)

    o.should_not be_respond_to(:x=)
    o.x = 34
    o.x.should == 34
    o.should be_respond_to(:x=)
  end

  it "should raise for a column that doesn't exist in the dataset" do
    o = @c.new

    proc {o.x}.should_not raise_error
    proc {o.xx}.should raise_error(Sequel::Error)

    proc {o.x = 3}.should_not raise_error
    proc {o.yy = 4}.should raise_error(Sequel::Error)

    proc {o.yy?}.should raise_error(NoMethodError)
  end

  it "should not raise for a column not in the dataset, but for which there's a value" do
    o = @c.new

    proc {o.xx}.should raise_error(Sequel::Error)
    proc {o.yy}.should raise_error(Sequel::Error)

    o.values[:xx] = 123
    o.values[:yy] = nil

    proc {o.xx; o.yy}.should_not raise_error(Sequel::Error)

    o.xx.should == 123
    o.yy.should == nil

    proc {o.xx = 3}.should raise_error(Sequel::Error)
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

  it "should raise for boolean argument (mistaken comparison)" do
    # This in order to prevent stuff like Model[:a == 'b']
    proc {@c[:a == 1]}.should raise_error(Sequel::Error)
    proc {@c[:a != 1]}.should raise_error(Sequel::Error)
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
    /^SELECT \* FROM items WHERE (\(node_id = 3921\) AND \(kind = 201\))|(\(kind = 201\) AND \(node_id = 3921\)) LIMIT 1$/
  end
end
