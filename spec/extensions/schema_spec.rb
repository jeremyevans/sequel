require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "dataset & schema" do
  before do
    @model = Class.new(Sequel::Model(:items))
  end 

  specify "sets schema with implicit table name" do
    @model.set_schema do
      primary_key :ssn, :string
    end
    @model.primary_key.should == :ssn
    @model.table_name.should == :items
  end

  specify "sets schema with explicit table name" do
    @model.set_schema :foo do
      primary_key :id
    end
    @model.primary_key.should == :id
    @model.table_name.should == :foo
  end
end

describe Sequel::Model, "table_exists?" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model(:items))
  end

  it "should get the table name and question the model's db if table_exists?" do
    @model.should_receive(:table_name).and_return(:items)
    @model.db.should_receive(:table_exists?)
    @model.table_exists?
  end
end

describe Sequel::Model, "create_table and schema" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model) do
      set_schema(:items) do
        text :name
        float :price, :null => false
      end
    end
  end

  it "should get the create table SQL list from the db and execute it line by line" do
    @model.create_table
    MODEL_DB.sqls.should == ['CREATE TABLE items (name text, price float NOT NULL)']
  end

  it "should reload the schema from the database" do
    schem = {:name=>{:type=>:string}, :price=>{:type=>:float}}
    @model.db.should_receive(:schema).with(:items, :reload=>true).and_return(schem.to_a.sort_by{|x| x[0].to_s})
    @model.create_table
    @model.db_schema.should == schem
    @model.instance_variable_get(:@columns).should == [:name, :price]
  end

  it "should return the schema generator via schema" do
    @model.schema.should be_a_kind_of(Sequel::Schema::Generator)
  end

  it "should use the superclasses schema if it exists" do
    @submodel = Class.new(@model)
    @submodel.schema.should be_a_kind_of(Sequel::Schema::Generator)
  end

  it "should return nil if no schema is present" do
    @model = Class.new(Sequel::Model)
    @model.schema.should == nil
    @submodel = Class.new(@model)
    @submodel.schema.should == nil
  end
end

describe Sequel::Model, "drop_table" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model(:items))
  end

  it "should get the drop table SQL for the associated table and then execute the SQL." do
    @model.should_receive(:table_name).and_return(:items)
    @model.db.should_receive(:drop_table_sql).with(:items)
    @model.db.should_receive(:execute).and_return(:true)
    @model.drop_table
  end

end

describe Sequel::Model, "create_table!" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model(:items))
  end
  
  it "should drop table if it exists and then create the table" do
    @model.should_receive(:drop_table).and_return(true)
    @model.should_receive(:create_table).and_return(true)

    @model.create_table!
  end

end
