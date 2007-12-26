require File.join(File.dirname(__FILE__), "spec_helper")

describe "Model#save" do
  
  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
  end
  
  it "should insert a record for a new model instance" do
    o = @c.new(:x => 1)
    o.save
    
    MODEL_DB.sqls.first.should == "INSERT INTO items (x) VALUES (1)"
  end

  it "should update a record for an existing model instance" do
    o = @c.new(:id => 3, :x => 1)
    o.save
    
    MODEL_DB.sqls.first.should =~ 
      /UPDATE items SET (id = 3, x = 1|x = 1, id = 3) WHERE \(id = 3\)/
  end
  
  it "should update only the given columns if given" do
    o = @c.new(:id => 3, :x => 1, :y => nil)
    o.save(:y)
    
    MODEL_DB.sqls.first.should == "UPDATE items SET y = NULL WHERE (id = 3)"
  end
  
  it "should mark saved columns as not changed" do
    o = @c.new(:id => 3, :x => 1, :y => nil)
    o[:y] = 4
    o.changed_columns.should == [:y]
    o.save(:x)
    o.changed_columns.should == [:y]
    o.save(:y)
    o.changed_columns.should == []
  end
  
end

describe "Model#save_changes" do
  
  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
  end
  
  it "should do nothing if no changed columns" do
    o = @c.new(:id => 3, :x => 1, :y => nil)
    o.save_changes
    
    MODEL_DB.sqls.should be_empty
  end
  
  it "should update only changed columns" do
    o = @c.new(:id => 3, :x => 1, :y => nil)
    o.x = 2

    o.save_changes
    MODEL_DB.sqls.should == ["UPDATE items SET x = 2 WHERE (id = 3)"]
    o.save_changes
    o.save_changes
    MODEL_DB.sqls.should == ["UPDATE items SET x = 2 WHERE (id = 3)"]
    MODEL_DB.reset

    o.y = 4
    o.save_changes
    MODEL_DB.sqls.should == ["UPDATE items SET y = 4 WHERE (id = 3)"]
    o.save_changes
    o.save_changes
    MODEL_DB.sqls.should == ["UPDATE items SET y = 4 WHERE (id = 3)"]
  end
  
end

describe "Model#set" do
  
  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
  end
  
  it "should generate an update statement" do
    o = @c.new(:id => 1)
    o.set(:x => 1)
    MODEL_DB.sqls.first.should == "UPDATE items SET x = 1 WHERE (id = 1)"
  end
  
  it "should update attribute values" do
    o = @c.new(:id => 1)
    o.x.should be_nil
    o.set(:x => 1)
    o.x.should == 1
  end
  
  it "should be aliased by #update" do
    o = @c.new(:id => 1)
    o.update(:x => 1)
    MODEL_DB.sqls.first.should == "UPDATE items SET x = 1 WHERE (id = 1)"
  end
end


describe "Model#new?" do
  
  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
    end
  end
  
  it "should be true for a new instance" do
    n = @c.new(:x => 1)
    n.should be_new
  end
  
  it "should be false after saving" do
    n = @c.new(:x => 1)
    n.save
    n.should_not be_new
  end
  
  it "should alias new_record? to new?" do
    n = @c.new(:x => 1)
    n.should respond_to(:new_record?)
    n.should be_new_record
    n.save
    n.should_not be_new_record
  end
  
end

describe Sequel::Model, "w/ primary key" do
  
  it "should default to ':id'" do
    model_a = Class.new Sequel::Model
    model_a.primary_key.should be_equal(:id)
  end

  it "should be changed through 'set_primary_key'" do
    model_a = Class.new(Sequel::Model) { set_primary_key :a }
    model_a.primary_key.should be_equal(:a)
  end

  it "should support multi argument composite keys" do
    model_a = Class.new(Sequel::Model) { set_primary_key :a, :b }
    model_a.primary_key.should be_eql([:a, :b])
  end

  it "should accept single argument composite keys" do
    model_a = Class.new(Sequel::Model) { set_primary_key [:a, :b] }
    model_a.primary_key.should be_eql([:a, :b])
  end
  
end

describe Sequel::Model, "w/o primary key" do
  it "should return nil for primary key" do
    Class.new(Sequel::Model) { no_primary_key }.primary_key.should be_nil
  end

  it "should raise a Sequel::Error on 'this'" do
    instance = Class.new(Sequel::Model) { no_primary_key }.new
    proc { instance.this }.should raise_error(Sequel::Error)
  end
end

describe Sequel::Model, "with this" do

  before { @example = Class.new Sequel::Model(:examples) }

  it "should return a dataset identifying the record" do
    instance = @example.new :id => 3
    instance.this.sql.should be_eql("SELECT * FROM examples WHERE (id = 3) LIMIT 1")
  end

  it "should support arbitary primary keys" do
    @example.set_primary_key :a

    instance = @example.new :a => 3
    instance.this.sql.should be_eql("SELECT * FROM examples WHERE (a = 3) LIMIT 1")
  end

  it "should support composite primary keys" do
    @example.set_primary_key :x, :y
    instance = @example.new :x => 4, :y => 5

    parts = [
      'SELECT * FROM examples WHERE %s LIMIT 1',
      '(x = 4) AND (y = 5)', 
      '(y = 5) AND (x = 4)'
    ].map { |expr| Regexp.escape expr }
    regexp = Regexp.new parts.first % "(?:#{parts[1]}|#{parts[2]})"

    instance.this.sql.should match(regexp)
  end

end

describe "Model#pk" do
  
  before(:each) do
    @m = Class.new(Sequel::Model)
  end
  
  it "should be default return the value of the :id column" do
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk.should == 111
  end

  it "should be return the primary key value for custom primary key" do
    @m.set_primary_key :x
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk.should == 2
  end

  it "should be return the primary key value for composite primary key" do
    @m.set_primary_key [:y, :x]
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk.should == [3, 2]
  end

  it "should raise if no primary key" do
    @m.set_primary_key nil
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk}.should raise_error(Sequel::Error)

    @m.no_primary_key
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk}.should raise_error(Sequel::Error)
  end
  
end

describe "Model#pk_hash" do

  before(:each) do
    @m = Class.new(Sequel::Model)
  end
  
  it "should be default return the value of the :id column" do
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_hash.should == {:id => 111}
  end

  it "should be return the primary key value for custom primary key" do
    @m.set_primary_key :x
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_hash.should == {:x => 2}
  end

  it "should be return the primary key value for composite primary key" do
    @m.set_primary_key [:y, :x]
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_hash.should == {:y => 3, :x => 2}
  end

  it "should raise if no primary key" do
    @m.set_primary_key nil
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk_hash}.should raise_error(Sequel::Error)

    @m.no_primary_key
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk_hash}.should raise_error(Sequel::Error)
  end
end

describe Sequel::Model, "create_with_params" do

  before(:each) do
    MODEL_DB.reset
    
    @c = Class.new(Sequel::Model(:items)) do
      def self.columns; [:x, :y]; end
    end
  end
  
  it "should filter the given params using the model columns" do
    @c.create_with_params(:x => 1, :z => 2)
    MODEL_DB.sqls.first.should == "INSERT INTO items (x) VALUES (1)"

    MODEL_DB.reset
    @c.create_with_params(:y => 1, :abc => 2)
    MODEL_DB.sqls.first.should == "INSERT INTO items (y) VALUES (1)"
  end
  
  it "should be aliased by create_with" do
    @c.create_with(:x => 1, :z => 2)
    MODEL_DB.sqls.first.should == "INSERT INTO items (x) VALUES (1)"

    MODEL_DB.reset
    @c.create_with(:y => 1, :abc => 2)
    MODEL_DB.sqls.first.should == "INSERT INTO items (y) VALUES (1)"
  end
  
end

describe Sequel::Model, "#destroy" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model(:items))
    @model.dataset.meta_def(:delete) {MODEL_DB.execute delete_sql}
    
    @instance = @model.new(:id => 1234)
    #@model.stub!(:delete).and_return(:true)
  end

  it "should run within a transaction" do
    @model.db.should_receive(:transaction)
    @instance.destroy
  end

  it "should run before_destroy and after_destroy hooks" do
    @model.before_destroy {MODEL_DB.execute('before blah')}
    @model.after_destroy {MODEL_DB.execute('after blah')}
    @instance.destroy
    
    MODEL_DB.sqls.should == [
      "before blah",
      "DELETE FROM items WHERE (id = 1234)",
      "after blah"
    ]
  end
end

describe Sequel::Model, "#exists?" do

  before(:each) do
    @model = Class.new(Sequel::Model(:items))
    @model_a = @model.new
  end

  it "should returns true when current instance exists" do
    @model_a.exists?.should be_true
  end

  it "should return false when the current instance does not exist" do
    pending("how would this be false?")
  end

end