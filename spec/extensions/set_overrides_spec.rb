require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Dataset #set_defaults" do
  before do
    @ds = Sequel.mock.dataset.from(:items).extension(:set_overrides).set_defaults(:x=>1)
  end

  it "should set the default values for inserts" do
    @ds.insert_sql.must_equal "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:x=>2).must_equal "INSERT INTO items (x) VALUES (2)"
    @ds.insert_sql(:y=>2).must_match(/INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/)
    @ds.set_defaults(:y=>2).insert_sql.must_match(/INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/)
    @ds.set_defaults(:x=>2).insert_sql.must_equal "INSERT INTO items (x) VALUES (2)"
  end

  it "should set the default values for updates" do
    @ds.update_sql.must_equal "UPDATE items SET x = 1"
    @ds.update_sql(:x=>2).must_equal "UPDATE items SET x = 2"
    @ds.update_sql(:y=>2).must_match(/UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/)
    @ds.set_defaults(:y=>2).update_sql.must_match(/UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/)
    @ds.set_defaults(:x=>2).update_sql.must_equal "UPDATE items SET x = 2"
  end

  it "should not affect String update arguments" do
    @ds.update_sql('y = 2').must_equal "UPDATE items SET y = 2"
  end

  # SEQUEL5: Remove
  unless Sequel.mock.dataset.frozen?
    it "should have working mutation method" do
      @ds = Sequel.mock.dataset.from(:items).extension(:set_overrides)
      @ds.set_defaults!(:x=>1)
      @ds.insert_sql.must_equal "INSERT INTO items (x) VALUES (1)"
    end
  end
end

describe "Sequel::Dataset #set_overrides" do
  before do
    @ds = Sequel.mock.dataset.from(:items).extension(:set_overrides).set_overrides(:x=>1)
  end

  it "should override the given values for inserts" do
    @ds.insert_sql.must_equal "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:x=>2).must_equal "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:y=>2).must_match(/INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/)
    @ds.set_overrides(:y=>2).insert_sql.must_match(/INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/)
    @ds.set_overrides(:x=>2).insert_sql.must_equal "INSERT INTO items (x) VALUES (1)"
  end

  it "should override the given values for updates" do
    @ds.update_sql.must_equal "UPDATE items SET x = 1"
    @ds.update_sql(:x=>2).must_equal "UPDATE items SET x = 1"
    @ds.update_sql(:y=>2).must_match(/UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/)
    @ds.set_overrides(:y=>2).update_sql.must_match(/UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/)
    @ds.set_overrides(:x=>2).update_sql.must_equal "UPDATE items SET x = 1"
  end

  # SEQUEL5: Remove
  unless Sequel.mock.dataset.frozen?
    it "should have working mutation method" do
      @ds = Sequel.mock.dataset.from(:items).extension(:set_overrides)
      @ds.set_overrides!(:x=>1)
      @ds.insert_sql.must_equal "INSERT INTO items (x) VALUES (1)"
    end
  end

  it "should consider dataset with select overrides and default a simple select all" do
    @ds.send(:simple_select_all?).must_equal true
  end
end
