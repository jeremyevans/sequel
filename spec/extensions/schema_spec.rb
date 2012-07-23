require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "dataset & schema" do
  before do
    @model = Class.new(Sequel::Model(:items))
    @model.plugin :schema
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

describe Sequel::Model, "create_table and schema" do
  before do
    @model = Class.new(Sequel::Model)
    @model.class_eval do
      plugin :schema
      set_schema(:items) do
        text :name
        float :price, :null => false
      end
    end
    MODEL_DB.reset
  end

  it "should get the create table SQL list from the db and execute it line by line" do
    @model.create_table
    MODEL_DB.sqls.should == ['CREATE TABLE items (name text, price float NOT NULL)']
  end

  it "should allow setting schema and creating the table in one call" do
    @model.create_table { text :name }
    MODEL_DB.sqls.should == ['CREATE TABLE items (name text)']
  end

  it "should reload the schema from the database" do
    schem = {:name=>{:type=>:string}, :price=>{:type=>:float}}
    @model.db.should_receive(:schema).with(@model.dataset, :reload=>true).and_return(schem.to_a.sort_by{|x| x[0].to_s})
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
    @model.plugin :schema
    @model.schema.should == nil
    @submodel = Class.new(@model)
    @submodel.schema.should == nil
  end
end

describe Sequel::Model, "schema methods" do
  before do
    @model = Class.new(Sequel::Model(:items))
    @model.plugin :schema
    MODEL_DB.reset
  end

  it "table_exists? should get the table name and question the model's db if table_exists?" do
    @model.db.should_receive(:table_exists?).and_return(false)
    @model.table_exists?.should == false
  end

  it "drop_table should drop the related table" do
    @model.drop_table
    MODEL_DB.sqls.should == ['DROP TABLE items']
  end

  it "drop_table? should drop the table if it exists" do
    @model.drop_table?
    MODEL_DB.sqls.should == ["SELECT NULL FROM items LIMIT 1", 'DROP TABLE items']
  end
  
  it "create_table! should drop table if it exists and then create the table" do
    @model.create_table!
    MODEL_DB.sqls.should == ["SELECT NULL FROM items LIMIT 1", 'DROP TABLE items', 'CREATE TABLE items ()']
  end
  
  it "create_table? should not create the table if it already exists" do
    @model.should_receive(:table_exists?).and_return(true)
    @model.create_table?
    MODEL_DB.sqls.should == []
  end

  it "create_table? should create the table if it doesn't exist" do
    @model.should_receive(:table_exists?).and_return(false)
    @model.create_table?
    MODEL_DB.sqls.should == ['CREATE TABLE items ()']
  end
end
