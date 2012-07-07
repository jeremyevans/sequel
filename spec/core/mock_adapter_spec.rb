require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel Mock Adapter" do
  specify "should have an adapter method" do
    db = Sequel.mock
    db.should be_a_kind_of(Sequel::Mock::Database)
    db.adapter_scheme.should == :mock
  end

  specify "should have constructor accept no arguments" do
    Sequel::Mock::Database.new.should be_a_kind_of(Sequel::Mock::Database)
  end

  specify "should each not return any rows by default" do
    called = false
    Sequel.mock[:t].each{|r| called = true}
    called.should be_false
  end

  specify "should return 0 for update/delete/with_sql_delete/execute_dui by default" do
    Sequel.mock[:t].update(:a=>1).should == 0
    Sequel.mock[:t].delete.should == 0
    Sequel.mock[:t].with_sql_delete('DELETE FROM t').should == 0
    Sequel.mock.execute_dui('DELETE FROM t').should == 0
  end

  specify "should return nil for insert/execute_insert by default" do
    Sequel.mock[:t].insert(:a=>1).should be_nil
    Sequel.mock.execute_insert('INSERT INTO a () DEFAULT VALUES').should be_nil
  end

  specify "should be able to set the rows returned by each using :fetch option with a single hash" do
    rs = []
    db = Sequel.mock(:fetch=>{:a=>1})
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>1}]
    db[:t].each{|r| r[:a] = 2; rs << r}
    rs.should == [{:a=>1}, {:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>1}, {:a=>2}, {:a=>1}]
  end

  specify "should be able to set the rows returned by each using :fetch option with an array of hashes" do
    rs = []
    db = Sequel.mock(:fetch=>[{:a=>1}, {:a=>2}])
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>1}, {:a=>2}]
    db[:t].each{|r| r[:a] += 2; rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}, {:a=>1}, {:a=>2}]
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
    db[:t].each{|r| r[:b] += 1; rs << r}
    db[:b].each{|r| r[:a] += 2; rs << r}
    rs.should == [{:b=>1}, {:a=>1}, {:a=>2}, {:b=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    db[:b].each{|r| rs << r}
    rs.should == [{:b=>1}, {:a=>1}, {:a=>2}, {:b=>2}, {:a=>3}, {:a=>4}, {:b=>1}, {:a=>1}, {:a=>2}]
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

  specify "should be able to set an exception to raise by setting the :fetch option to an exception class " do
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
    db = Sequel.mock(:fetch=>[{:a=>1}, [{:a=>2}, {:a=>3}], proc{|s| {:a=>4}}, proc{|s| }, nil, ArgumentError])
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.should == [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    proc{db[:t].all}.should raise_error(Sequel::DatabaseError)
  end

  specify "should be able to set the rows returned by each on a per dataset basis using _fetch" do
    rs = []
    db = Sequel.mock(:fetch=>{:a=>1})
    ds = db[:t]
    ds.each{|r| rs << r}
    rs.should == [{:a=>1}]
    ds._fetch = {:b=>2}
    ds.each{|r| rs << r}
    rs.should == [{:a=>1}, {:b=>2}]
  end

  specify "should raise Error if given an invalid object to fetch" do
    proc{Sequel.mock(:fetch=>Class.new).get(1)}.should raise_error(Sequel::Error)
    proc{Sequel.mock(:fetch=>Object.new).get(1)}.should raise_error(Sequel::Error)
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

  specify "should be able to set the number of rows modified by update and delete on a per dataset basis" do
    db = Sequel.mock(:numrows=>2)
    ds = db[:t]
    ds.update(:a=>1).should == 2
    ds.delete.should == 2
    ds.numrows = 3
    ds.update(:a=>1).should == 3
    ds.delete.should == 3
  end

  specify "should raise Error if given an invalid object for numrows or autoid" do
    proc{Sequel.mock(:numrows=>Class.new)[:a].delete}.should raise_error(Sequel::Error)
    proc{Sequel.mock(:numrows=>Object.new)[:a].delete}.should raise_error(Sequel::Error)
    proc{Sequel.mock(:autoid=>Class.new)[:a].insert}.should raise_error(Sequel::Error)
    proc{Sequel.mock(:autoid=>Object.new)[:a].insert}.should raise_error(Sequel::Error)
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

  specify "should be able to set the autogenerated primary key returned by insert on a per dataset basis" do
    db = Sequel.mock(:autoid=>1)
    ds = db[:t]
    ds.insert(:a=>1).should == 1
    ds.autoid = 5
    ds.insert(:a=>1).should == 5
    ds.insert(:a=>1).should == 6
    db[:t].insert(:a=>1).should == 2
  end

  specify "should be able to set the columns to set in the dataset as an array of symbols" do
    db = Sequel.mock(:columns=>[:a, :b])
    db[:t].columns.should == [:a, :b]
    db.sqls.should == ["SELECT * FROM t LIMIT 1"]
    ds = db[:t]
    ds.all
    db.sqls.should == ["SELECT * FROM t"]
    ds.columns.should == [:a, :b]
    db.sqls.should == []
    db[:t].columns.should == [:a, :b]
  end

  specify "should be able to set the columns to set in the dataset as an array of arrays of symbols" do
    db = Sequel.mock(:columns=>[[:a, :b], [:c, :d]])
    db[:t].columns.should == [:a, :b]
    db[:t].columns.should == [:c, :d]
  end

  specify "should be able to set the columns to set in the dataset as a proc" do
    db = Sequel.mock(:columns=>proc{|sql| (sql =~ / t/) ? [:a, :b] : [:c, :d]})
    db[:b].columns.should == [:c, :d]
    db[:t].columns.should == [:a, :b]
  end

  specify "should have a columns= method to set the columns to set after the fact" do
    db = Sequel.mock
    db.columns = [[:a, :b], [:c, :d]]
    db[:t].columns.should == [:a, :b]
    db[:t].columns.should == [:c, :d]
  end

  specify "should raise Error if given an invalid columns" do
    proc{Sequel.mock(:columns=>Object.new)[:a].columns}.should raise_error(Sequel::Error)
  end

  specify "should not quote identifiers by default" do
    Sequel.mock.send(:quote_identifiers_default).should be_false
  end

  specify "should allow overriding of server_version" do
    db = Sequel.mock
    db.server_version.should == nil
    db.server_version = 80102
    db.server_version.should == 80102
  end

  specify "should not have identifier input/output methods by default" do
    Sequel.mock.send(:identifier_input_method_default).should be_nil
    Sequel.mock.send(:identifier_output_method_default).should be_nil
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

  specify "should disconnect correctly" do
    db = Sequel.mock
    db.test_connection
    proc{db.disconnect}.should_not raise_error
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

  specify "should append :arguments option to execute to the SQL if present" do
    db = Sequel.mock
    db.execute('SELECT * FROM t', :arguments=>[1, 2])
    db.sqls.should == ['SELECT * FROM t -- args: [1, 2]']
  end

  specify "should have Dataset#columns take columns to set and return self" do
    db = Sequel.mock
    ds = db[:t].columns(:id, :a, :b)
    ds.should be_a_kind_of(Sequel::Mock::Dataset)
    ds.columns.should == [:id, :a, :b]
  end

  specify "should be able to load dialects based on the database name" do
    begin
      qi = class Sequel::Database; @@quote_identifiers; end
      ii = class Sequel::Database; @@identifier_input_method; end
      io = class Sequel::Database; @@identifier_output_method; end
      Sequel.quote_identifiers = nil
      class Sequel::Database; @@identifier_input_method=nil; end
      class Sequel::Database; @@identifier_output_method=nil; end
      Sequel.mock(:host=>'access').select(Date.new(2011, 12, 13)).sql.should == 'SELECT #2011-12-13#'
      Sequel.mock(:host=>'db2').select(1).sql.should == 'SELECT 1 FROM "SYSIBM"."SYSDUMMY1"'
      Sequel.mock(:host=>'firebird')[:a].distinct.limit(1, 2).sql.should == 'SELECT DISTINCT FIRST 1 SKIP 2 * FROM "A"'
      Sequel.mock(:host=>'informix')[:a].distinct.limit(1, 2).sql.should == 'SELECT SKIP 2 FIRST 1 DISTINCT * FROM A'
      Sequel.mock(:host=>'mssql')[:a].full_text_search(:b, 'c').sql.should == "SELECT * FROM [A] WHERE (CONTAINS ([B], 'c'))"
      Sequel.mock(:host=>'mysql')[:a].full_text_search(:b, 'c').sql.should == "SELECT * FROM `a` WHERE (MATCH (`b`) AGAINST ('c'))"
      Sequel.mock(:host=>'oracle')[:a].limit(1).sql.should == 'SELECT * FROM (SELECT * FROM "A") "T1" WHERE (ROWNUM <= 1)'
      Sequel.mock(:host=>'postgres')[:a].full_text_search(:b, 'c').sql.should == "SELECT * FROM \"a\" WHERE (to_tsvector('simple'::regconfig, (COALESCE(\"b\", ''))) @@ to_tsquery('simple'::regconfig, 'c'))"
      Sequel.mock(:host=>'sqlite')[:a___b].sql.should == "SELECT * FROM `a` AS 'b'"
    ensure
      Sequel.quote_identifiers = qi
      Sequel::Database.send(:class_variable_set, :@@identifier_input_method, ii)
      Sequel::Database.send(:class_variable_set, :@@identifier_output_method, io)
    end
  end

  specify "should automatically set version for postgres" do
    Sequel.mock(:host=>'postgres').server_version.should == 90103
  end

  specify "should stub out the primary_key method for postgres" do
    Sequel.mock(:host=>'postgres').primary_key(:t).should == :id
  end
end
