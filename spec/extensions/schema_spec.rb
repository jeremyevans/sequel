require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "set_schema" do
  before do
    @model = Class.new(Sequel::Model(:items))
    @model.plugin :schema
  end 

  it "sets schema with implicit table name" do
    @model.set_schema do
      primary_key :ssn, :type=>:string
    end
    @model.primary_key.must_equal :ssn
    @model.table_name.must_equal :items
  end

  it "sets schema with explicit table name" do
    @model.set_schema :foo do
      primary_key :id
    end
    @model.primary_key.must_equal :id
    @model.table_name.must_equal :foo
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
    DB.reset
  end

  it "should get the create table SQL list from the db and execute it line by line" do
    @model.create_table
    DB.sqls.must_equal ['CREATE TABLE items (name text, price float NOT NULL)']
  end

  it "should allow setting schema and creating the table in one call" do
    @model.create_table { text :name }
    DB.sqls.must_equal ['CREATE TABLE items (name text)']
  end

  it "should reload the schema from the database" do
    schem = {:name=>{:type=>:string}, :price=>{:type=>:float}}
    @model.db.stub(:schema, schem.to_a.sort_by{|x| x[0].to_s}) do
      @model.create_table
      @model.db_schema.must_equal schem
    end
    @model.instance_variable_get(:@columns).must_equal [:name, :price]
  end

  it "should return the schema generator via schema" do
    @model.schema.must_be_kind_of(Sequel::Schema::Generator)
  end

  it "should use the superclasses schema if it exists" do
    @submodel = Class.new(@model)
    @submodel.schema.must_be_kind_of(Sequel::Schema::Generator)
  end

  it "should return nil if no schema is present" do
    @model = Class.new(Sequel::Model)
    @model.plugin :schema
    @model.schema.must_equal nil
    @submodel = Class.new(@model)
    @submodel.schema.must_equal nil
  end
end

describe Sequel::Model, "schema methods" do
  before do
    @model = Class.new(Sequel::Model(:items))
    @model.plugin :schema
    DB.reset
  end

  it "table_exists? should get the table name and question the model's db if table_exists?" do
    DB.stub(:table_exists?, false) do
      @model.table_exists?.must_equal false
    end
  end

  it "drop_table should drop the related table" do
    @model.drop_table
    DB.sqls.must_equal ['DROP TABLE items']
  end

  it "drop_table? should drop the table if it exists" do
    @model.drop_table?
    DB.sqls.must_equal ["SELECT NULL AS nil FROM items LIMIT 1", 'DROP TABLE items']
  end
  
  it "create_table! should drop table if it exists and then create the table" do
    @model.create_table!
    DB.sqls.must_equal ["SELECT NULL AS nil FROM items LIMIT 1", 'DROP TABLE items', 'CREATE TABLE items ()']
  end
  
  it "create_table? should not create the table if it already exists" do
    DB.stub(:table_exists?, true) do
      @model.create_table?
    end
    DB.sqls.must_equal []
  end

  it "create_table? should create the table if it doesn't exist" do
    DB.stub(:table_exists?, false) do
      @model.create_table?
    end
    DB.sqls.must_equal ['CREATE TABLE items ()']
  end
end
