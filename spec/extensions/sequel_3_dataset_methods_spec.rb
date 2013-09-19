require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Dataset#to_csv" do
  before do
    @ds = Sequel.mock(:fetch=>[{:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}, {:a=>7, :b=>8, :c=>9}])[:items].columns(:a, :b, :c).extension(:sequel_3_dataset_methods)
  end

  specify "should format a CSV representation of the records" do
    @ds.to_csv.should == "a, b, c\r\n1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n"
  end

  specify "should exclude column titles if so specified" do
    @ds.to_csv(false).should == "1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n"
  end
end

describe "Dataset#[]=" do
  specify "should perform an update on the specified filter" do
    db = Sequel.mock
    ds = db[:items].extension(:sequel_3_dataset_methods)
    ds[:a => 1] = {:x => 3}
    db.sqls.should == ['UPDATE items SET x = 3 WHERE (a = 1)']
  end
end

describe "Dataset#insert_multiple" do
  before do
    @db = Sequel.mock(:autoid=>2)
    @ds = @db[:items].extension(:sequel_3_dataset_methods)
  end

  specify "should insert all items in the supplied array" do
    @ds.insert_multiple(['aa', 5, 3, {:a => 2}])
    @db.sqls.should == ["INSERT INTO items VALUES ('aa')",
      "INSERT INTO items VALUES (5)",
      "INSERT INTO items VALUES (3)",
      "INSERT INTO items (a) VALUES (2)"]
  end

  specify "should pass array items through the supplied block if given" do
    @ds.insert_multiple(["inevitable", "hello", "the ticking clock"]){|i| i.gsub('l', 'r')}
    @db.sqls.should == ["INSERT INTO items VALUES ('inevitabre')",
      "INSERT INTO items VALUES ('herro')",
      "INSERT INTO items VALUES ('the ticking crock')"]
  end

  specify "should return array of inserted ids" do
    @ds.insert_multiple(['aa', 5, 3, {:a => 2}]).should == [2, 3, 4, 5]
  end

  specify "should work exactly like in metioned in the example" do
    @ds.insert_multiple([{:x=>1}, {:x=>2}]){|row| row[:y] = row[:x] * 2 ; row }
    sqls = @db.sqls
    ["INSERT INTO items (x, y) VALUES (1, 2)", "INSERT INTO items (y, x) VALUES (2, 1)"].should include(sqls[0])
    ["INSERT INTO items (x, y) VALUES (2, 4)", "INSERT INTO items (y, x) VALUES (4, 2)"].should include(sqls[1])
  end
end

describe "Dataset#db=" do
  specify "should change the dataset's database" do
    db = Sequel.mock
    ds = db[:items].extension(:sequel_3_dataset_methods)
    db2 = Sequel.mock
    ds.db = db2
    ds.db.should == db2
    ds.db.should_not == db
  end
end

describe "Dataset#opts=" do
  specify "should change the dataset's opts" do
    db = Sequel.mock
    ds = db[:items].extension(:sequel_3_dataset_methods)
    db2 = Sequel.mock
    ds.sql.should == 'SELECT * FROM items'
    ds.opts = {}
    ds.sql.should == 'SELECT *'
    ds.opts.should == {}
  end
end

describe "Dataset#set" do
  specify "should act as alias to #update" do
    db = Sequel.mock
    ds = db[:items].extension(:sequel_3_dataset_methods)
    ds.set({:x => 3})
    db.sqls.should == ['UPDATE items SET x = 3']
  end
end

describe "Sequel::Dataset#qualify_to_first_source" do
  specify "should qualify to the first source" do
    Sequel.mock.dataset.extension(:sequel_3_dataset_methods).from(:t).filter{a<b}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a < t.b)'
  end
end

describe "Sequel::Dataset#qualify_to" do
  specify "should qualify to the given table" do
    Sequel.mock.dataset.extension(:sequel_3_dataset_methods).from(:t).filter{a<b}.qualify_to(:e).sql.should == 'SELECT e.* FROM t WHERE (e.a < e.b)'
  end
end

