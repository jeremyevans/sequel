require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Dataset #set_defaults" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items).extension(:set_overrides).set_defaults(:x=>1)
  end

  specify "should set the default values for inserts" do
    @ds.insert_sql.should == "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:x=>2).should == "INSERT INTO items (x) VALUES (2)"
    @ds.insert_sql(:y=>2).should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_defaults(:y=>2).insert_sql.should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_defaults(:x=>2).insert_sql.should == "INSERT INTO items (x) VALUES (2)"
  end

  specify "should set the default values for updates" do
    @ds.update_sql.should == "UPDATE items SET x = 1"
    @ds.update_sql(:x=>2).should == "UPDATE items SET x = 2"
    @ds.update_sql(:y=>2).should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_defaults(:y=>2).update_sql.should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_defaults(:x=>2).update_sql.should == "UPDATE items SET x = 2"
  end
end

describe "Sequel::Dataset #set_overrides" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items).extension(:set_overrides).set_overrides(:x=>1)
  end

  specify "should override the given values for inserts" do
    @ds.insert_sql.should == "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:x=>2).should == "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:y=>2).should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_overrides(:y=>2).insert_sql.should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_overrides(:x=>2).insert_sql.should == "INSERT INTO items (x) VALUES (1)"
  end

  specify "should override the given values for updates" do
    @ds.update_sql.should == "UPDATE items SET x = 1"
    @ds.update_sql(:x=>2).should == "UPDATE items SET x = 1"
    @ds.update_sql(:y=>2).should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_overrides(:y=>2).update_sql.should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_overrides(:x=>2).update_sql.should == "UPDATE items SET x = 1"
  end
end
