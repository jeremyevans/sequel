require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel Mock Adapter" do
  specify "should have an adapter method" do
    db = Sequel.mock
    db.should be_a_kind_of(Sequel::Mock::Database)
    db.adapter_scheme.should == :mock
  end

  specify "should each not return any rows by default" do
    called = false
    Sequel.mock[:t].each{|r| called = true}
    called.should be_false
  end

  specify "should return 0 for update and delete by default" do
    Sequel.mock[:t].update(:a=>1).should == 0
    Sequel.mock[:t].delete.should == 0
  end

  specify "should return nil for insert by default" do
    Sequel.mock[:t].insert(:a=>1).should be_nil
  end

  specify "should be able to set the rows returned by each using :fetch option with a single hash" do
    rs = []
    db = Sequel.mock(:fetch=>{:a=>1})
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}] * 2
  end

  specify "should be able to set the rows returned by each using :fetch option with an array of hashes" do
    rs = []
    db = Sequel.mock(:fetch=>[{:a=>1}, {:a=>2}])
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}] * 2
  end

  specify "should be able to set the rows returned by each using :fetch option with an array or arrays of hashes" do
    rs = []
    db = Sequel.mock(:fetch=>[[{:a=>1}, {:a=>2}], [{:a=>3}, {:a=>4}]])
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
  end

  specify "should be able to set the rows returned by each using :fetch option with a proc that takes sql" do
    rs = []
    db = Sequel.mock(:fetch=>proc{|sql| sql =~ /FROM t/ ? {:b=>1} : [{:a=>1}, {:a=>2}]})
    db[:t].each{|r| rs << r}
    rs.should == [{:b=>1}]
    db[:b].each{|r| rs << r}
    rs.should == [{:b=>1}, {:a=>1}, {:a=>2}]
  end

  specify "should have a fetch= method for setting rows returned by each after the fact" do
    rs = []
    db = Sequel.mock
    db.fetch = {:a=>1}
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}] * 2
  end

  specify "should be able to set an exception to raise by setting the :numrows option to an exception class " do
    db = Sequel.mock(:fetch=>ArgumentError)
    proc{db[:t].all}.should raise_error(Sequel::DatabaseError)
    begin
      db[:t].all
    rescue => e
    end
    e.should be_a_kind_of(Sequel::DatabaseError)
    e.wrapped_exception.should be_a_kind_of(ArgumentError) 
  end

  specify "should be able to set separate kinds of results for fetch using an array" do
    rs = []
    db = Sequel.mock(:fetch=>[{:a=>1}, [{:a=>2}, {:a=>3}], proc{|s| {:a=>4}}, nil, ArgumentError])
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    proc{db[:t].all}.should raise_error(Sequel::DatabaseError)
  end

  specify "should be able to set the number of rows modified by update and delete using :numrows option as an integer" do
    db = Sequel.mock(:numrows=>2)
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 2
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 2
  end

  specify "should be able to set the number of rows modified by update and delete using :numrows option as an array of integers" do
    db = Sequel.mock(:numrows=>[2, 1])
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 1
    db[:t].update(:a=>1).should == 0
    db[:t].delete.should == 0
  end

  specify "should be able to set the number of rows modified by update and delete using :numrows option as a proc" do
    db = Sequel.mock(:numrows=>proc{|sql| sql =~ / t/ ? 2 : 1})
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 2
    db[:b].update(:a=>1).should == 1
    db[:b].delete.should == 1
  end

  specify "should be able to set an exception to raise by setting the :numrows option to an exception class " do
    db = Sequel.mock(:numrows=>ArgumentError)
    proc{db[:t].update(:a=>1)}.should raise_error(Sequel::DatabaseError)
    begin
      db[:t].delete
    rescue => e
    end
    e.should be_a_kind_of(Sequel::DatabaseError)
    e.wrapped_exception.should be_a_kind_of(ArgumentError) 
  end

  specify "should be able to set separate kinds of results for numrows using an array" do
    db = Sequel.mock(:numrows=>[1, proc{|s| 2}, nil, ArgumentError])
    db[:t].delete.should == 1
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 0
    proc{db[:t].delete}.should raise_error(Sequel::DatabaseError)
  end

  specify "should have a numrows= method to set the number of rows modified by update and delete after the fact" do
    db = Sequel.mock
    db.numrows = 2
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 2
    db[:t].update(:a=>1).should == 2
    db[:t].delete.should == 2
  end

  specify "should be able to set the autogenerated primary key returned by insert using :autoid option as an integer" do
    db = Sequel.mock(:autoid=>1)
    db[:t].insert(:a=>1).should == 1
    db[:t].insert(:a=>1).should == 2
    db[:t].insert(:a=>1).should == 3
  end

  specify "should be able to set the autogenerated primary key returned by insert using :autoid option as an array of integers" do
    db = Sequel.mock(:autoid=>[1, 3, 5])
    db[:t].insert(:a=>1).should == 1
    db[:t].insert(:a=>1).should == 3
    db[:t].insert(:a=>1).should == 5
    db[:t].insert(:a=>1).should be_nil
  end

  specify "should be able to set the autogenerated primary key returned by insert using :autoid option as a proc" do
    db = Sequel.mock(:autoid=>proc{|sql| sql =~ /INTO t / ? 2 : 1})
    db[:t].insert(:a=>1).should == 2
    db[:t].insert(:a=>1).should == 2
    db[:b].insert(:a=>1).should == 1
    db[:b].insert(:a=>1).should == 1
  end

  specify "should be able to set an exception to raise by setting the :autoid option to an exception class " do
    db = Sequel.mock(:autoid=>ArgumentError)
    proc{db[:t].insert(:a=>1)}.should raise_error(Sequel::DatabaseError)
    begin
      db[:t].insert
    rescue => e
    end
    e.should be_a_kind_of(Sequel::DatabaseError)
    e.wrapped_exception.should be_a_kind_of(ArgumentError) 
  end

  specify "should be able to set separate kinds of results for autoid using an array" do
    db = Sequel.mock(:autoid=>[1, proc{|s| 2}, nil, ArgumentError])
    db[:t].insert.should == 1
    db[:t].insert.should == 2
    db[:t].insert.should == nil
    proc{db[:t].insert}.should raise_error(Sequel::DatabaseError)
  end

  specify "should have an autoid= method to set the autogenerated primary key returned by insert after the fact" do
    db = Sequel.mock
    db.autoid = 1
    db[:t].insert(:a=>1).should == 1
    db[:t].insert(:a=>1).should == 2
    db[:t].insert(:a=>1).should == 3
  end

  specify "should keep a record of all executed SQL in #sqls" do
    db = Sequel.mock
    db[:t].all
    db[:b].delete
    db[:c].insert(:a=>1)
    db[:d].update(:a=>1)
    db.sqls.should == ['SELECT * FROM t', 'DELETE FROM b', 'INSERT INTO c (a) VALUES (1)', 'UPDATE d SET a = 1']
  end

  specify "should clear sqls on retrieval" do
    db = Sequel.mock
    db[:t].all
    db.sqls.should == ['SELECT * FROM t']
    db.sqls.should == []
  end

  specify "should also log SQL executed to the given loggers" do
    a = []
    def a.method_missing(m, *x) push(*x) end
    db = Sequel.mock(:loggers=>[a])
    db[:t].all
    db[:b].delete
    db[:c].insert(:a=>1)
    db[:d].update(:a=>1)
    a.should == ['SELECT * FROM t', 'DELETE FROM b', 'INSERT INTO c (a) VALUES (1)', 'UPDATE d SET a = 1']
  end

  specify "should correctly handle transactions" do
    db = Sequel.mock
    db.transaction{db[:a].all}
    db.sqls.should == ['BEGIN', 'SELECT * FROM a', 'COMMIT']
    db.transaction{db[:a].all; raise Sequel::Rollback}
    db.sqls.should == ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    proc{db.transaction{db[:a].all; raise ArgumentError}}.should raise_error(ArgumentError)
    db.sqls.should == ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    proc{db.transaction(:rollback=>:reraise){db[:a].all; raise Sequel::Rollback}}.should raise_error(Sequel::Rollback)
    db.sqls.should == ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    db.transaction(:rollback=>:always){db[:a].all}
    db.sqls.should == ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    db.transaction{db.transaction{db[:a].all; raise Sequel::Rollback}}
    db.sqls.should == ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    db.transaction{db.transaction(:savepoint=>true){db[:a].all; raise Sequel::Rollback}}
    db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'SELECT * FROM a', 'ROLLBACK TO SAVEPOINT autopoint_1', 'COMMIT']
    db.transaction{db.transaction(:savepoint=>true){db[:a].all}; raise Sequel::Rollback}
    db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'SELECT * FROM a', 'RELEASE SAVEPOINT autopoint_1', 'ROLLBACK']
  end

  specify "should correctly handle transactions when sharding" do
    db = Sequel.mock(:servers=>{:test=>{}})
    db.transaction{db.transaction(:server=>:test){db[:a].all; db[:t].server(:test).all}}
    db.sqls.should == ['BEGIN', 'BEGIN -- test', 'SELECT * FROM a', 'SELECT * FROM t -- test', 'COMMIT -- test', 'COMMIT']
  end

  specify "should yield a mock connection object from synchronize" do
    c = Sequel.mock.synchronize{|conn| conn}
    c.should be_a_kind_of(Sequel::Mock::Connection)
  end

  specify "should deal correctly with sharding" do
    db = Sequel.mock(:servers=>{:test=>{}})
    c1 = db.synchronize{|conn| conn}
    c2 = db.synchronize(:test){|conn| conn}
    c1.server.should == :default
    c2.server.should == :test
  end

  specify "should accept :extend option for extending the object with a module" do
    Sequel.mock(:extend=>Module.new{def foo(v) v * 2 end}).foo(3).should == 6
  end

  specify "should accept :sqls option for where to store the SQL queries" do
    a = []
    Sequel.mock(:sqls=>a)[:t].all
    a.should == ['SELECT * FROM t']
  end

  specify "should include :append option in SQL if it is given" do
    db = Sequel.mock(:append=>'a')
    db[:t].all
    db.sqls.should == ['SELECT * FROM t -- a']
  end

  specify "should have Dataset#columns take columns to set and return self" do
    db = Sequel.mock
    ds = db[:t].columns(:id, :a, :b)
    ds.should be_a_kind_of(Sequel::Mock::Dataset)
    ds.columns.should == [:id, :a, :b]
  end
end
