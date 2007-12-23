require File.join(File.dirname(__FILE__), "../spec_helper")

Sequel::Model.db = MODEL_DB = MockDatabase.new

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

describe Sequel::Model, "create_table" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model(:items))
  end

  it "should get the create table SQL list from the db and execute it line by line" do
    #db.create_table_sql_list(table_name, *schema.create_info).each {|s| db << s} 
    @model.should_receive(:table_name).and_return(:items)
    @model.schema.should_receive(:create_info)
    @model.db.should_receive(:create_table_sql_list)
    pending("Finish specing this")
    @model.create_table
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
    @model.should_receive(:table_exists?).and_return(true)
    @model.should_receive(:drop_table).and_return(true)
    @model.should_receive(:create_table).and_return(true)

    @model.create_table!
  end

end

describe Sequel::Model, "recreate_table" do

  before(:each) do
    MODEL_DB.reset
    @model = Class.new(Sequel::Model(:items))
  end

  it "should raise a depreciation warning and then call create_table!" do
    @model.should_receive(:warn)
    @model.should_receive(:create_table!).and_return(true)
    @model.recreate_table
  end

end
